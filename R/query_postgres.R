#' query_postgres
#' @description Funzione che esegue una query data in input in Postgres
#' @param query `chr` query da eseguire su Postgres.
#' @returns `tibble`
#' @export

query_postgres <- function(query){
  
  con <- connect2postgres()
  result <- dbFetch( dbSendQuery(con, query) )
  dbDisconnect(con)
  
  return(tibble(result))
}