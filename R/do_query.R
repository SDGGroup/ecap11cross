#' do_query
#' @description Funzione che esegue una query data in input
#' @param .con `S4 object` usato per comunicare con il database
#' @param .query `chr` query da eseguire su Postgres.
#' @returns `tibble`
#' @export

do_query <- function(.con, .query){
  
  result <- dbFetch( dbSendQuery(.con, .query) )
  
  return(tibble(result))
}