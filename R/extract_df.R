#' extract_df
#' @description Estrae una specifica versione (`.table_version`) dalla tabella (`.table_version`)
#' in BigQuery, filtrata per una specifica `.dat_report`
#' @param .con un oggetto output di \link{connect2bq}, usato per comunicare con il database 
#' @param .table_name `chr` nome della tabella
#' @param .table_version `int` versione della tabella
#' @param .dat_report `date (%Y-%m-%d)`
#' @param .order_input `vector` vettore di `chr` contenente l'ordinamento delle colonne. Se non specificato 
#' restituisce l'ordinamento di default di Big Query 
#' @returns `tibble`
#' @export

extract_df <- function(.con, .table_name, .table_version, .dat_report, .order_input = NULL){
  
  # estrae tabella
  df <- tbl(.con, .table_name)
  
  # filtra per versione e dat_report
  df <- df %>% 
    filter(ID_VERSIONE == .table_version &
             DAT_REPORT == .dat_report) %>%
    select(-c(ID_VERSIONE, DAT_REPORT)) %>% 
    collect()
  
  # aggiungi order by se valore non nullo
  if (!(is.null(.order_input))){
    df <- df %>% 
      select(.order_input)
  }
  
  return(df)
}
