#' retrieve_data_catalog.R
#' @description Esegue il download dei controlli di data quality da Postgres in base al processo indicato.
#' @param cod_processo chr, codice del processo per il quale sono richiesti i controlli.
#' @param catalog_table tibble, tabella Postgres contenente i controlli di data quality.
#' @param project_id chr, nome del progetto GCP.
#' @returns Una tibble con 13 colonne:
#' * COD_CONTROLLO  chr,
#' * COD_PROCESSO chr,
#' * COD_TIPO_CONTROLLO chr,
#' * COD_FREQ_CONTROLLO chr,
#' * COD_SEVERITA chr,
#' * DES_CONTROLLO chr,
#' * MSG chr,
#' * STMT_CONTROLLO chr,
#' * FLG_ATTIVO chr,
#' * TMS_CREAZIONE ?,
#' * DAT_FINE_VALIDITA ?,
#' * COD_UTENTE_MODIFICA ?,
#' * COD_UTENTE_CREAZIONE chr.
#' @export

retrieve_data_catalog <- function(cod_processo, catalog_table){

  query <- glue("SELECT * FROM {catalog_table} WHERE cod_processo='{cod_processo}'")
  catalog <- query_postgres(query) %>% 
    rename_with(~ str_to_upper(.x), everything())

  return(catalog)
  
}    