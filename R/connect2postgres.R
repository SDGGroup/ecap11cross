#' connect2postgres
#' @description Crea una connessione al database `ddl-dcerm-db` su Postgres 
#' @returns Un S4 object ereditato da \link[DBI]{dbConnect}, usato per comunicare con il database
#' @export

connect2postgres <- function(){
  
  con <- dbConnect(
    RPostgres::Postgres(), 
    host = '100.125.240.3',
    port = '5432',
    dbname ='ddl-dcerm-db',
    user = "ddl_dcerm_app",
    password = "dcerm_app"
  )
  
  return(con)
}

