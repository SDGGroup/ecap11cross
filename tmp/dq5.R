# query associata al DQ5:
# Verifica che il campo DES_SHOCK della tabella TE_IRRBB_SHIFT_SENSITIVITY contenga un unico valore se COD_PERIMETRO='NO_PATH_DEPENDENT'
# 
# SELECT  COMPLETO.output1, RISULTATO.conteggio FROM  (
#   SELECT COUNT(1) as conteggio,   
#                 1 as riferimento 
#   FROM  `prj-isp-ddle0-sand-prod-002.ds_ecap.TE_IRRBB_SHIFT_SENSITIVITY` 
#   WHERE DAT_REPORT ='2022-12-31'   
#   AND ID_VERSIONE = 0
# ) RISULTATO 
# LEFT JOIN (
#   SELECT count(DES_SHOCK) AS output1,  
#                         1 as riferimento 
#   FROM (   
#     SELECT     distinct DES_SHOCK as  DES_SHOCK 
#     FROM     `prj-isp-ddle0-sand-prod-002.ds_ecap.TE_IRRBB_SHIFT_SENSITIVITY` 
#     WHERE DAT_REPORT ='2022-12-31'   AND ID_VERSIONE = 0 and COD_PERIMETRO = 'NO_PATH_DEPENDENT'
#   ) 
#   HAVING count(DES_SHOCK)>1 
#  ) COMPLETO 
# ON (COMPLETO.riferimento = RISULTATO.riferimento)

# output:
# A tibble: 1 x 2
# output1 conteggio
# <int>     <int>
#  NA    164160

rm(list = ls(all.names = T))
library(dbplyr)
library(dplyr)
library(stringr)
library(glue)
library(bigrquery)
library(bigQueryR)
library(DBI)

# setup
project_id <- "prj-isp-ddle0-sand-prod-002"
dataset <- "ds_ecap"
dat_report <- '2022-12-31'
out_version <- 0
caricamento_shift_sensitivity <- 'FEEDING_AUTOMATICO'
caricamento_discount_factor   <- 'CARICAMENTO_MANUALE'
versione_shift_sensitivity      <- 0
versione_shift_sensitivity_base <- 3
versione_discount_factor        <- 1
versione_mapping_des_shock      <- 1
versione_mapping_currency       <- 1
DATASET_GENERAL <- "ds_ddl_general"
TAB_DIAGNOSTICA <- "TE_DIAGNOSTICA"

# parametri nel catalog
cod_processo <- "ecap11_irrbb_notional_equivalent"
cod_severita <- "ERROR"
cod_controllo <- "5"
des_controllo <-"Verifica che il campo DES_SHOCK della tabella TE_IRRBB_SHIFT_SENSITIVITY contenga un unico valore se COD_PERIMETRO='NO_PATH_DEPENDENT'"
msg <- "Il campo DES_SHOCK presente in TE_IRRBB_SHIFT_SENSITIVITY {dat_report} v{versione_shift_sensitivity} non contiene un unico valore quando COD_PERIMETRO='NO_PATH_DEPENDENT', ma ne contiene {output1}"


# funzione per connettersi a big query
connect2bq <- function(.project_id, .dataset){
  
  # autenticazzione a big query
  bqr_auth()
  
  # connessione a big query
  con <- dbConnect(
    bigrquery::bigquery(),
    project = .project_id,
    dataset = .dataset
  )
  
  return(con)
}

# funzione per scrivere df su bq
writedf2bq <- function(.project_id, 
                       .dataframe, 
                       .dataset, 
                       .tabella, 
                       .out_version,
                       .dat_report,
                       .tab_schema = NULL){
  
  dataframe <- .dataframe %>% 
    mutate(ID_VERSIONE = as.integer(.out_version),
           DAT_REPORT = as.Date(.dat_report)
    )
  
  if(is.null(.tab_schema)){
    tab_schema <- schema_fields(dataframe)
  }
  
  bqr_upload_data(projectId         = .project_id,
                  datasetId         = .dataset,
                  tableId           = .tabella,
                  upload_data       = dataframe,
                  create            = 'CREATE_IF_NEEDED',
                  writeDisposition  = 'WRITE_APPEND',
                  wait              = FALSE,
                  schema            = tab_schema)
  
}


# funzione che modifica df_errors per avere la struttura della TA_DIAGNOSTICA
mutate_dferrors <- function(.df_errors, 
                            .dat_report,
                            .out_version,
                            .cod_processo, 
                            .cod_severita, 
                            .cod_controllo, 
                            .des_controllo,
                            .msg){
  
  conteggio_record_ko <- nrow(.df_errors)
  
  # TODO: nel caso in cui si debba partire dalla tabella catalog
  # cols2bind <- check_row %>%
  #   select(COD_PROCESSO,
  #          COD_SEVERITA,
  #          COD_CONTROLLO,
  #          DES_CONTROLLO,
  #          MSG)
  # 
  # df_errors <- df_errors %>%
  #   bind_cols(col2bind) %>%
  #   mutate(DAT_REPORT      = .dat_report,
  #          ID_VERSIONE     = as.integer(.out_version),
  #          DAT_INSERIMENTO = Sys.Date(),
  #          DES_ESITO       = MSG
  #   )
  
  df_errors <- .df_errors %>%
    mutate(COD_PROCESSO    = .cod_processo,
           COD_SEVERITA    = .cod_severita,
           COD_CONTROLLO   = .cod_controllo,
           DES_CONTROLLO   = .des_controllo,
           DAT_REPORT      = .dat_report,
           ID_VERSIONE     = as.integer(.out_version),
           DAT_INSERIMENTO = Sys.Date(),
    ) 
  
  # sostituisce espressioni tra {} in DES_ESITO = .msg con glue
  # la priorità è data alle variabili della tibble e se non vengono trovate
  # usa le variabili nell'ambiente 
  df_errors <- df_errors %>% 
    mutate(DES_ESITO = as.character(glue(.msg)))
  
  # TODO: perche' avevano messo con sapply?
  # df_errors$DES_ESITO <- sapply(as.character(df_errors$DES_ESITO)  , function (x) glue(x))
  
  # eliminiamo se ci sono, le colonne che non sono richieste dalla tabella TE_DIAGNOSTICA
  df_errors <- df_errors %>% 
    select(-starts_with("output"))
  
  return(df_errors)
}

dq5 <- function(.project_id, 
                .dataset,
                .dat_report,
                .out_version,
                .cod_processo, 
                .cod_severita, 
                .cod_controllo, 
                .des_controllo, 
                .msg,
                .dataset_general,
                .tab_diagnostica){
  
  message("Diagnostico 5 in esecuzione")
  
  # connect to bq
  con <- connect2bq(.project_id, .dataset)
  
  # retrieve tables used for DQ
  shift_sensitivity <- tbl(con, "TE_IRRBB_SHIFT_SENSITIVITY")
  
  # do DQ
  df1 <-  shift_sensitivity %>% 
    filter(DAT_REPORT == '2022-12-31' & 
             ID_VERSIONE == 0) %>% 
    summarise(conteggio = n(),
              riferimento = 1)
  
  df2 <- shift_sensitivity %>% 
    filter(DAT_REPORT == '2022-12-31' & 
             ID_VERSIONE == 0 & 
             COD_PERIMETRO == 'NO_PATH_DEPENDENT') %>% 
    distinct(DES_SHOCK) %>% 
    summarise(output1 = n(),
              riferimento = 1) %>% 
    filter(output1 >= 1)
  
  df_errors <- df1 %>% 
    left_join(df2, by = "riferimento") %>% 
    select(output1, conteggio) %>% 
    collect()
  
  # prepara df_error per scrittura 
  df_errors <- mutate_dferrors(df_errors, 
                               .dat_report,
                               .out_version,
                               .cod_processo, 
                               .cod_severita, 
                               .cod_controllo, 
                               .des_controllo, 
                               .msg)
  
  # scrive gli esiti dei controlli su BigQuery, tabella TE_DIAGNOSTICA
  tryCatch({writedf2bq(.project_id, 
                       df_errors,
                       .dataset_general, 
                       .tab_diagnostica,
                       .out_version,
                       .dat_report)},
           error = function(e) {
             message("Errore nella scrittura dell'esito controllo 5 in TA_DIAGNOSTICA")
           })
  
}

dq5(project_id, 
    dataset,
    dat_report,
    out_version,
    cod_processo, 
    cod_severita, 
    cod_controllo, 
    des_controllo, 
    msg,
    DATASET_GENERAL,
    TAB_DIAGNOSTICA)
