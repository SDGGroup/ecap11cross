#' connect2postgres
#' @description Crea una connessione al database `ddl-dcerm-db` su Postgres 
#' @param .host `chr`
#' @param .port `chr`
#' @param .dbname `chr`
#' @param .user `chr`
#' @param .password `chr`
#' @returns Un S4 object ereditato da \link[DBI]{dbConnect}, usato per comunicare con il database
#' @export

connect2postgres <- function(.host, .port, .dbname, .user, .password){
  
  con <- dbConnect(
    Postgres(), 
    host = .host,
    port = .port,
    dbname = .dbname,
    user = .user,
    password = .password
  )
  
  return(con)
}

