#' query_postgres
#' @description Funzione che esegue una query data in input in Postgres
#' @param query `chr` query da eseguire su Postgres.
#' @returns `tibble`
#' @export

query_postgres <- function(query){

  db <- create_conn()
  result <- dbFetch( dbSendQuery(db, query) )
  dbDisconnect(db)

  return(tibble(result))
}
