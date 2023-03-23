#' write_df_on_postgres
#' @description Funzione per l'inserimento di dati su Postgres
#' @param nome_tabella `chr` nome tabella Postgres di destinazione
#' @param oggetto `tibble` oggetto da scrivere
#' @param project_id `chr` nome del progetto GCP
#' @returns ?
#' @export

write_df_on_postgres <- function(nome_tabella,oggetto){

  # TODO: aggiungere cosa ritorna
  db <- create_conn()
  result <- dbWriteTable(db,nome_tabella,oggetto, append = TRUE)
  dbDisconnect(db)

  return(result)

}
