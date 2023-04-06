# remove rows created from tests 

rm(list = ls(all.names = T))
library(dbplyr)
library(dplyr)
library(stringr)
library(glue)
library(bigrquery)
library(bigQueryR)
library(DBI)
source("tmp/utils.R")

# bq
project_id <- "prj-isp-ddle0-sand-prod-002"
dataset <- "ds_ddl_general"
con <- connect2bq(project_id, dataset)

te_diagn <- tbl(con, "TE_DIAGNOSTICA")

records_to_remove = te_diagn %>%
  filter(COD_PROCESSO == "ecap11_irrbb_notional_equivalent",
         ID_VERSIONE == 9999)

query = sql_render(records_to_remove) %>%
  as.character() %>%
  str_replace("SELECT \\*", "DELETE")

DBI::dbExecute(con, query)

# postgres
con <- connect2postgres()

te_esito <- tbl(con, "te_esito_controllo_dq")

ex <- te_esito %>%
  filter(dat_report  == '2022-12-31', str_detect(cod_processo, "ecap11")) %>% 
  collect()
