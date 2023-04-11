#' writedf2postgres
#' @description Funzione per appendere un dataframe a una tabella del database `ddl-dcerm-db` su Postgres
#' @param .con un oggetto output di \link{connect2postgres}, usato per comunicare con il database 
#' @param .nome_tabella `chr` nome tabella di destinazione
#' @param .dataframe `tibble` tibble da scrivere
#' @returns `bool` TRUE, invisibilmente
#' @export

writedf2postgres <- function(.con, .nome_tabella, .dataframe){
  
  dbWriteTable(.con, .nome_tabella, .dataframe, append = TRUE)
  
}
