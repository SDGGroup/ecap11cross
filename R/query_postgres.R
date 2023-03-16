#' query_postgres.R
#' @description Funzione che esegue una query data in input in POSTGRES.
#' @param query chr, query da eseguire su Postgres.
#' @returns Una tibble con il risultato della query. 
#' @export

query_postgres <- function(query){
  
  db <- create_conn()
  result <- dbFetch( dbSendQuery(db, query) )
  dbDisconnect(db) 
  
  return(tibble(result))
}
