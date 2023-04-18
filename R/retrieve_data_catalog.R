#' retrieve_data_catalog
#' @description Esegue il download dei controlli di data quality da Postgres
#' (`.data_quality_postgres == "SI"`)  o carica
#' i controlli di data quality dal file in locale (`.data_quality_postgres == "NO"`) 
#' in base al processo indicato
#' @param .con  un oggetto output di \link{connect2postgres}, usato per comunicare con il database 
#' @param .cod_processo `chr` codice del processo per il quale sono richiesti i controlli
#' @param .data_quality_postgres `chr`
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

retrieve_data_catalog <- function(.con, .cod_processo, .data_quality_postgres){
  
  if(.data_quality_postgres == "SI"){
    ta_controllo_dq <- tbl(.con, "ta_controllo_dq")
    
    catalog <- ta_controllo_dq %>% 
      filter(cod_processo == .cod_processo) %>% 
      collect() 
  } else{
    catalog <- read_delim("config/data_quality_catalog.csv", ";", escape_double = FALSE, trim_ws = TRUE) 
  }

  return(catalog)

}
