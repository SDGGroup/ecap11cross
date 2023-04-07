#' retrieve_data_catalog
#' @description Esegue il download dei controlli di data quality da Postgres in base al processo indicato
#' @param .cod_processo `chr` codice del processo per il quale sono richiesti i controlli
#' @param .catalog_table `chr` nome tabella Postgres contenente i controlli di data quality
#' @param project_id `chr` nome del progetto GCP
#' @returns `tibble`:
#' * COD_CONTROLLO  `chr`
#' * COD_PROCESSO `chr`
#' * COD_TIPO_CONTROLLO `chr`
#' * COD_FREQ_CONTROLLO `chr`
#' * COD_SEVERITA `chr`
#' * DES_CONTROLLO `chr`
#' * MSG `chr`
#' * STMT_CONTROLLO `chr`
#' * FLG_ATTIVO `chr`
#' * TMS_CREAZIONE ?
#' * DAT_FINE_VALIDITA ?
#' * COD_UTENTE_MODIFICA ?
#' * COD_UTENTE_CREAZIONE `chr`
#' @export

retrieve_data_catalog <- function(.cod_processo){

  con <- connect2postgres()
  
  ta_controllo_dq <- tbl(con, "ta_controllo_dq")

  catalog <- ta_controllo_dq %>% 
    filter(cod_processo == .cod_processo) %>% 
    collect()

  dbDisconnect(con)

  return(catalog)

}
