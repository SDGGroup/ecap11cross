#' create_conn
#' @description Crea una connessione a Postgres
#' @returns Un S4 object ereditato da \link[DBI]{dbConnect}, usato per comunicare con il database
#' @export

create_conn <- function(){

  con <- dbConnect(Postgres(),
                   dbname =  DBNAME_POSTGRES,
                   host = HOST_POSTGRES,
                   port = PORT_POSTGRES,
                   user = USER_POSTGRES,
                   password = PASSWORD_POSTGRES)

  return(con)
}
