#' create_conn.R
#' @description Crea una connessione a POSTEGRES.
#' @returns Un S4 object ereditato da \link[DBI]{dbConnect}, usato per comunicare con il database. 
#' @export

create_conn <- function(){

  con <- DBI::dbConnect(RPostgres::Postgres(),                       
                        dbname =  DBNAME_POSTGRES,
                        host = HOST_POSTGRES,
                        port = PORT_POSTGRES,
                        user = USER_POSTGRES,
                        password = PASSWORD_POSTGRES)
  
  return(con)
}
