#' writedf2bq
#' @description Funzione per l'inserimento di dati su Big Query 
#' @param .project_id `chr` nome progetto
#' @param .dataframe `tibble` tibble da scrivere
#' @param .dataset `chr` dataset di destinazione
#' @param .tabella `chr` tabella di destinazione 
#' @param .out_version `int` ID_VERSIONE da inserire nel dataframe
#' @param .dat_report `chr` DAT_REPORT da inserire nel dataframe
#' @param .tab_schema uno schema object che può essere passato come argomento di \link[bigQueryR]{bqr_upload_data}
#' @returns `bool` TRUE, invisibilmente. 
#' @export

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
  
  # TODO: Questo non è un errore vero:
  # i 2023-03-31 11:15:13 > Request Status Code:  404
  # Error: API returned
  # il df viene scritto correttamente 
  bqr_upload_data(projectId         = .project_id,
                  datasetId         = .dataset,
                  tableId           = .tabella,
                  upload_data       = dataframe,
                  create            = 'CREATE_IF_NEEDED',
                  writeDisposition  = 'WRITE_APPEND',
                  wait              = FALSE,
                  schema            = tab_schema)
  
}
