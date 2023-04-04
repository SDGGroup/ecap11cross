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
source("tmp/utils.R")
source("tmp/check_interfaccia_functions.R")
source("tmp/do_dq.R")
source("tmp/dq_5.R")
source("tmp/dq_5error.R")
source("tmp/dq_b.R")

# setup
project_id <- "prj-isp-ddle0-sand-prod-002"
dataset <- "ds_ecap"
out_version <- 10000
# TODO: ricordarsi di cancellare tutte i dq scritti durante il test
caricamento_shift_sensitivity <- 'FEEDING_AUTOMATICO'
caricamento_discount_factor   <- 'CARICAMENTO_MANUALE'
dataset_general <- "ds_ddl_general"
tab_diagnostica <- "TE_DIAGNOSTICA"
# parametri per glue
versione_shift_sensitivity <- 0
dat_report <- '2022-12-31'

# parametri nel catalog
cod_processo <- "ecap11_irrbb_notional_equivalent"
cod_severita <- "ERROR"
cod_controllo <- "5"
des_controllo <-"Verifica che il campo DES_SHOCK della tabella TE_IRRBB_SHIFT_SENSITIVITY contenga un unico valore se COD_PERIMETRO='NO_PATH_DEPENDENT'"
# TODO: mettere i punti nelle variabili del msg?
msg <- "Il campo DES_SHOCK presente in TE_IRRBB_SHIFT_SENSITIVITY {dat_report} v{versione_shift_sensitivity} non contiene un unico valore quando COD_PERIMETRO='NO_PATH_DEPENDENT', ma ne contiene {output1}"

# DQ 5
do_dq(dq_5,
      project_id, 
      dataset,
      dat_report,
      out_version,
      cod_processo, 
      cod_severita, 
      cod_controllo, 
      des_controllo, 
      msg,
      dataset_general,
      tab_diagnostica)

# DQ 5 error
do_dq(dq_5error,
      project_id, 
      dataset,
      dat_report,
      out_version,
      cod_processo, 
      cod_severita, 
      cod_controllo, 
      des_controllo, 
      msg,
      dataset_general,
      tab_diagnostica)

# DQ 5 error - cod_severita WARNING
do_dq(dq_5error,
      project_id, 
      dataset,
      dat_report,
      out_version,
      cod_processo, 
      .cod_severita = "WARNING", 
      cod_controllo, 
      des_controllo, 
      msg,
      dataset_general,
      tab_diagnostica)

# Error: COPY returned error: ERROR:  duplicate key value violates unique constraint "te_esito_controllo_dq_pkey"
# DETAIL:  Key (cod_controllo, cod_processo, dat_report, num_versione)=(5, ecap11_irrbb_notional_equivalent, 2022-12-31, 9999) already exists.
# CONTEXT:  COPY te_esito_controllo_dq, line 2

# DQ b - check interfaccia 
msg <- "Nella tabella TE_IRRBB_SHIFT_SENSITIVITY, il campo COD_PERIMETRO può contenere solo i valori PATH_DEPENDENT e NO_PATH_DEPENDENT"
des_controllo <- "Nella tabella TE_IRRBB_SHIFT_SENSITIVITY {dat_report} v{versione_shift_sensitivity} la colonna COD_PERIMETRO contiene il valore {output2} per la chiave {output1}."

do_dq(dq_b,
      project_id, 
      dataset,
      dat_report,
      out_version,
      cod_processo, 
      cod_severita, 
      cod_controllo, 
      des_controllo, 
      msg,
      dataset_general,
      tab_diagnostica)
