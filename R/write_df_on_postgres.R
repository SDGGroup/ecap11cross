#' write_df_on_postgres.R
#' @description Funzione per l'inserimento di dati su POSTGRES.
#' @param nome_tabella chr, nome tabella Postgres di destinazione.
#' @param oggetto tibble, oggetto da scrivere.
#' @param project_id chr, nome del progetto GCP.
#' @returns Scrive i dati su Postgres.
#' @export

write_df_on_postgres <- function(nome_tabella,oggetto){

  db <- create_conn()
  result <- dbWriteTable(db,nome_tabella,oggetto, append = TRUE)
  dbDisconnect(db)
  
  return(result)
  
}
