rm(list = ls(all.names = T))
library(dbplyr)
library(dplyr)
library(stringr)
library(glue)
library(bigrquery)
library(bigQueryR)
library(DBI)
source("tmp/utils.R")
source("tmp/do_dq.R")
source("tmp/dq_5.R")
source("tmp/dq_8.R")
source("tmp/dq_5error.R")
source("tmp/dq_b.R")

# setup

# parametri specifici step
# TODO: fare più liste?
params_config <- list()

# generali step
project_id                      <- "prj-isp-ddle0-sand-prod-002"
dataset                         <- "ds_ecap"
out_version                     <-  10000
# specifici dq
versione_shift_sensitivity_base <- 3
versione_discount_factor        <- 1
versione_mapping_des_shock      <- 1
versione_shift_sensitivity      <- 0
dat_report                      <- '2022-12-31'

# crea lista con tutti i parametri nel global.env di tipo num o char
params_list <- ls()[sapply(mget(ls()), function(x) is.character(x) | is.numeric(x))]
params_config <- eval(parse(text = paste("params_config <- list(", paste(paste(params_list, params_list , sep=" = ") ,collapse = ", "), ")")))

# parametri nel catalog
cod_processo <- rep("ecap11_irrbb_notional_equivalent", 5)
cod_severita <- c("ERROR", "ERROR", "WARNING", "ERROR",  "ERROR")
cod_controllo <- c("5", "5", "5", "8", "b")
des_controllo <- c(
  rep("Verifica che il campo DES_SHOCK della tabella TE_IRRBB_SHIFT_SENSITIVITY contenga un unico valore se COD_PERIMETRO='NO_PATH_DEPENDENT'", 3),
  "Verifica che tutti i DES_SHOCK presenti nella tabella TE_IRRBB_SHIFT_SENSITIVITY_BASE siano presenti nel campo DES_SHOCK_SHIFT_SENSITIVITY della tabella TE_IRRBB_MAPPING_DES_SHOCK.",
  "Nella tabella TE_IRRBB_SHIFT_SENSITIVITY, il campo COD_PERIMETRO può contenere solo i valori “PATH_DEPENDENT” e “NO_PATH_DEPENDENT”"
)
  
msg <- c(
  rep("Il campo DES_SHOCK presente in TE_IRRBB_SHIFT_SENSITIVITY {dat_report} v{versione_shift_sensitivity} non contiene un unico valore quando COD_PERIMETRO='NO_PATH_DEPENDENT', ma ne contiene {output1}", 3),
  "Il DES_SHOCK {output1} risulta presente in TE_IRRBB_SHIFT_SENSITIVITY_BASE {dat_report} v{versione_shift_sensitivity_base} ma non in DES_SHOCK_SHIFT_SENSITIVITY della tabella TE_IRRBB_MAPPING_DES_SHOCK {dat_report} v{versione_mapping_des_shock}",
  "Nella tabella TE_IRRBB_SHIFT_SENSITIVITY {dat_report} v{versione_shift_sensitivity} la colonna COD_PERIMETRO contiene il valore {output2} per la chiave {output1}" 
)

dq <- c(dq_5, dq_5error, dq_5error, dq_8, dq_b)

catalog <- tibble(cod_processo, cod_severita, cod_controllo, des_controllo, msg, dq)
  


# connette a big query 
con <- connect2bq(.project_id = params_config$project_id,
                  .dataset    = params_config$dataset)

# esegue data quality 
debug(do_dq)
catalog %>% 
  rowwise() %>% 
  summarise(esito = do_dq(dq, 
                          cod_processo,
                          cod_severita, 
                          cod_controllo, 
                          des_controllo, 
                          msg, 
                          con, 
                          params_config)
  )
