#!/usr/bin/env Rscript

# CARICAMENTO LIBRERIE \ FILES --------------------------------------------
rm(list=ls(all = TRUE)) 
# Caricamento librerie necessarie per interagire con GCP
library(bigrquery)           # funzioni di download\upload
library(bigQueryR)           # funzioni di autenticazione: bqr_auth
library(gargle)              # necessaria per l'autenticazione con la gcp
library(googleCloudStorageR) # necessaria per l'upload dei file

# Caricamento librerie necessarie per interagire con Postgres
library(RPostgres)   # libreria PostgreSql
library(DBI)         # libreria gestione DataBase relazionali
library(RPostgreSQL)

# Caricamento librerie cross processo
library(readr)
library(ini)
library(logging)
library(stringr)     # serve per str_replace_all
library(glue)        # serve per leggere le stringhe parametrizzate del csv di DQ

# Caricamento librerie specifiche per il processo
library(dplyr)       # server per join, ecc ...
library(lubridate)   # contiene floor_date
#library(tidyverse)
library(tidyr)

require(ecap11s6)
require(ecap11cross)

# CARICAMENTO SCRIPT -----------------------------------------------------------

# Caricamento funzioni cross processi. Necessario definire il path "code/..."
# source("code/data_quality.R")
# source("code/input.R")
# source("code/output.R")
# source("code/utils.R")
# source("code/check_interfaccia.R")           # contiene la funzione che esegue i controlli da interfaccia
# source("code/set_env_variables.R")

# Caricamento funzioni specifiche del processo 



# GESTIONE LANCIO --------------------------------------------------------------

lancio_interfaccia     <- 'NO'
# 'SI': legge i parametri ed il config.ini da airflow
# 'NO': parametri da settare e config.ini della cartella config

link_postgres_locale   <- 'NO' 
# 'SI': variabili d'ambiente prese da load_environment_local() (PC locale) 
# 'NO': variabili d'ambiente prese da load_environment()     (VM, Airflow) 

data_quality_postgres   <- 'SI' 
# 'SI': scarica i controlli dalla tabella TA_CONTROLLO_DQ di PostGres
# 'NO': legge il csv data_quality_catalog.csv nella cartella config

recupero_param_postgres <- 'NO'
# 'SI': recupero parametri procedura da interfaccia con id_elaborazione
# 'NO': setting  parametri procedura manuale

tempistica              <- 'SI'
# 'SI': restituisce i tempi di elaborazione del runner
# 'NO': non fa nulla

if(tempistica=='SI')
{
  library(tictoc)
  tic()
}

# DEFINIZIONE FUNZIONE SCRITTURA LOG -------------------------------------------

if (lancio_interfaccia != "SI") {
  out2log <- out2log_local
} else {
  out2log <- out2log_airflow
}


# CARICAMENTO PARAMETRI INPUT --------------------------------------------------

out2log("Caricamento parametri in input \n")

if(lancio_interfaccia=='SI')
{
  # In caso di sviluppo commentare questa parte
  args = commandArgs(trailingOnly = TRUE)
  if (length(args) < 4) {
    stop("Quattro argomenti attesi:id_elaborazione|out_version|dat_rep|codice_utente", call. = FALSE)
  } else{
    id_elaborazione <- as.integer(args[1])
    out_version <- as.integer(args[2])
    dat_report <- as.Date(args[3])
    codice_utente <- args[4]
  }
} else {
  
  # Utilizzata solo per gli sviluppi
  # caricamento manuale per sviluppo
  
  id_elaborazione <- 0
  dat_report <- as.Date('2022-12-31')
  out_version <- as.integer(0)
  codice_utente <- "origin"
}

# Variabile per eseguire l'update su POSTGRES relativo alla tabelle di ELABORAZIONE
code_status <- 'EXECUTED'  


# LETTURA E CARICAMENTO PARAMETRI DA config.ini --------------------------------

# Le variabili caricate da config e le variabili di ambiente sono identificate dal fatto
# di essere definite in MAIUSCOLO;
# le altre variabili create dal processo sono definite in minuscolo


# lettura file  config.ini (da testare con Airflow)
config <- read.ini("/Users/emanuele_depaoli/dev/ecap11_irrbb_ecap/config/config.ini")


# Assegnazione variabili per leggere in GCP
PROJECT_ID         <- config$GCP$project_id 
COD_PROCESSO       <- config$GCP$cod_processo
DATASET_GENERAL    <- config$GCP$dataset_general
DATASET_ECAP       <- config$GCP$dataset_ecap

# Assegnazione variabili (nomi tabelle) globali
# Non modificare il nome perchè entrano nelle funzioni cross procedure!

# BigQuery 
DEFAULT_VALUES     <- config$BQ_TABLES$default_values
TAB_DIAGNOSTICA    <- config$BQ_TABLES$diagnostica

# Postgres
CATALOG_TABLE      <- config$POSTGRESQL_TABLES$catalog_table


# Assegnazione variabili (nomi tabelle) specifiche

# Input
CURVE_1Y                 <- config$BQ_TABLES$curve_1y
NOTIONAL_EQUIVALENT      <- config$BQ_TABLES$notional_equivalent
NOTIONAL_EQUIVALENT_BASE <- config$BQ_TABLES$notional_equivalent_base
SHOCK_EFFETTIVI          <- config$BQ_TABLES$shock_effettivi
MAPPING_ENTITY           <- config$BQ_TABLES$mapping_entity

# Output
ECAP      <- config$BQ_TABLES$ecap
CURVE_VAR <- config$BQ_TABLES$curve_var
DELTA_PV  <- config$BQ_TABLES$delta_pv

# CARICAMENTO VARIABILI AMBIENTE -----------------------------------------------

if(lancio_interfaccia != 'SI'){
  
  out2log('Caricamento variabili di ambiente \n')
  
  if(link_postgres_locale=='SI' && lancio_interfaccia !="SI")
  {
    load_environment_local() 
  }
  if(link_postgres_locale=='NO' && lancio_interfaccia !="SI")
  {
    load_environment() 
  }
}
# i nomi di queste variabili devono coincidere (anche maiuscolo\minuscolo)
# con quelli utilizzati nella funzione code/input.R::retrieve_data_catalog()
DBNAME_POSTGRES   <- Sys.getenv('dbname_postgres')
HOST_POSTGRES     <- Sys.getenv('host_postgres')
PORT_POSTGRES     <- Sys.getenv('port_postgres')
USER_POSTGRES     <- Sys.getenv('user_postgres')
PASSWORD_POSTGRES <- Sys.getenv('password_postgres')


# RICHIAMO DEI PARAMETRI POSTGRES ----------------------------------------------

out2log('Richiamo parametri da PostGres \n')

# Funzione di connessione a Postgres per il richiamo dei parametri inseriti da interfaccia 
# i parametri recuperati da Postgres sono relativi sia ai parametri del modello che alle versioni delle tabelle

if(lancio_interfaccia=='SI') {
  
  params                            <- retrieve_params(id_elaborazione)
  secondo_percentile                <- as.double(params[params$param_nome == 'secondo_percentile',]$param_value)
  scenario_no_prepayment            <- '+100' #params[params$param_nome == 'scenario_no_prepayment',]$param_value #TODO
  prepayment                        <- params[params$param_nome == 'prepayment',]$param_value
  mesi_tenor_prepayment             <- params[params$param_nome == 'mesi_tenor_prepayment',]$param_value
  formula_delta_pv                  <- params[params$param_nome == 'formula_delta_pv',]$param_value
  storicizza_delta_pv               <- params[params$param_nome == 'storicizza_delta_pv',]$param_value
  versione_curve_1y                 <- params[params$param_nome == CURVE_1Y,]$param_value 
  versione_notional_equivalent      <- params[params$param_nome == NOTIONAL_EQUIVALENT,]$param_value 
  versione_notional_equivalent_base <- params[params$param_nome == NOTIONAL_EQUIVALENT_BASE,]$param_value
  versione_shock_effettivi          <- params[params$param_nome == SHOCK_EFFETTIVI,]$param_value 
  versione_mapping_entity           <- params[params$param_nome == MAPPING_ENTITY,]$param_value 
} else {
  
  if(recupero_param_postgres == 'SI'){
    
    params                            <- retrieve_params(id_elaborazione)
    secondo_percentile                <- as.double(params[params$param_nome == 'secondo_percentile',]$param_value)
    scenario_no_prepayment            <- '+100' #params[params$param_nome == 'scenario_no_prepayment',]$param_value #TODO
    prepayment                        <- params[params$param_nome == 'prepayment',]$param_value
    mesi_tenor_prepayment             <- params[params$param_nome == 'mesi_tenor_prepayment',]$param_value
    formula_delta_pv                  <- params[params$param_nome == 'formula_delta_pv',]$param_value
    storicizza_delta_pv               <- params[params$param_nome == 'storicizza_delta_pv',]$param_value
    versione_curve_1y                 <- params[params$param_nome == CURVE_1Y,]$param_value 
    versione_notional_equivalent      <- params[params$param_nome == NOTIONAL_EQUIVALENT,]$param_value 
    versione_notional_equivalent_base <- params[params$param_nome == NOTIONAL_EQUIVALENT_BASE,]$param_value
    versione_shock_effettivi          <- params[params$param_nome == SHOCK_EFFETTIVI,]$param_value 
    versione_mapping_entity           <- params[params$param_nome == MAPPING_ENTITY,]$param_value 
  } else{
    
    # Setting manuale 
    # Parametri specifici della procedura
    secondo_percentile     <- 0.96
    scenario_no_prepayment <- '+100'
    prepayment             <- 'SI'
    mesi_tenor_prepayment  <- 180
    formula_delta_pv       <- 'GESTIONALE' # GESTIONALE/SEGNALETICA
    storicizza_delta_pv    <- 'NO'
    
    # versioni lancio sviluppo
    versione_curve_1y                 <- 1      
    versione_notional_equivalent      <- 18
    versione_notional_equivalent_base <- 18
    versione_shock_effettivi          <- 18
    versione_mapping_entity           <- 1
  }
}

# CONNESSIONE BIGQUERY ---------------------------------------------------------

bqr_auth()
bqr_global_project(PROJECT_ID)
client_projectId  = bqr_get_global_project()


# DATA QUALITY -----------------------------------------------------------------

out2log('Inizio Data Quality Propedeutico\n')               # Inizio DQ antecedente il caricamento dell'input.

# Download  dei controlli di Data Quality 

if(data_quality_postgres=='SI')
{
  # Download da Postgres 
  catalog <- retrieve_data_catalog(COD_PROCESSO, CATALOG_TABLE)
  
} else {
  # Download da csv locale
  catalog <- read_delim("config/data_quality_catalog.csv", ";", escape_double = FALSE, trim_ws = TRUE) 
}

# Esecuzione dei controlli di data quality
# attraverso la funzione DQ.
# La funzione DQ restituisce TRUE se c'è errore FALSE se non c'è errore.
# 
# Sono definite tre tipologie di controlli:
# - ERROR....: se DQ restituisce TRUE (i.e. presenza di errore nel controllo ) dev'essere bloccata l'esecuzione del runner. 
# - WARNING..: se DQ restituisce TRUE (i.e. presenza di warning nel controllo) NON dev'essere bloccata l'esecuzione del runner. 
# - INFO.....: se DQ restituisce TRUE (i.e. presenza di warning nel controllo) NON dev'essere bloccata l'esecuzione del runner.

# Inizializziamo questa variabile prima di eseguire tutti i DQ di tipo ERROR antecedenti l'upload degli input!
# In questo modo, prima di bloccare l'esecuzione del runner scriviamo in TA_DIAGNOSTICA tutti gli errori bloccanti antecedenti l'upload degli input.

#### DQ ERROR ####

esito_global <- FALSE

#### DQ1 ####
#Verifica che tutti i COD_VALUTA_FINALE presenti in TE_IRRBB_NOTIONAL_EQUIVALENT 
#siano anche presenti nel campo COD_VALUTA della tabella TE_IRRBB_CURVE_1Y

esito <- do_DQ(check       = 1,
               catalog     = catalog,
               project_id  = PROJECT_ID,
               out_version = out_version,
               df_errors   = NULL)

if (esito) {out2log('ERRORE Data Quality 1 \n')}

esito_global <- esito

