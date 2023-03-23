#' write_df_2_BQ
#' @description Funzione per l'inserimento di dati su BIGQUERY
#' @param project_id `chr` progetto contenente la base dati sorgente
#' @param dataframe `tibble` dataframe da scrivere
#' @param dataset `chr` dataset di destinazione
#' @param tabella `chr` tabella di destinazione
#' @param tab_schema `list` lista con la stessa struttura dell'output schema_fields.
#'                          Se non specificato valore di default nullo e viene eseguito l'if
#' @export

write_df_2_BQ <- function(project_id, dataframe, dataset, tabella, tab_schema = NULL){

  dataframe$ID_VERSIONE <- as.integer(out_version)
  dataframe$DAT_REPORT  <- as.Date(dat_report)

  if(is.null(tab_schema)){
    tab_schema <- schema_fields(dataframe)
  }

  #funzione per la gestione di errori o avvisi
  tryCatch({bqr_upload_data(project_id,
                            dataset,
                            tabella,
                            dataframe,
                            create='CREATE_IF_NEEDED',
                            writeDisposition='WRITE_APPEND',
                            wait=FALSE,
                            schema = tab_schema)},error = function(e) {})

}
