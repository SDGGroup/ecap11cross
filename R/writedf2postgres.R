#' writedf2postgres
#' @description Funzione per appendere un dataframe a una tabella del database `ddl-dcerm-db` su Postgres
#' @param .nome_tabella `chr` nome tabella di destinazione
#' @param .dataframe `tibble` tibble da scrivere
#' @returns `bool` TRUE, invisibilmente. 
#' @export

writedf2postgres <- function(.nome_tabella, .dataframe){
  
  con <- connect2postgres()
  result <- dbWriteTable(con, .nome_tabella, .dataframe, append = TRUE)
  dbDisconnect(con)
  
}
