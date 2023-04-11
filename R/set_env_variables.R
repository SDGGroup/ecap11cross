#' load_environment
#' @description Crea le variabili d'ambiente per VM e Airflow
#' @export
load_environment <- function(){
  Sys.setenv(host_postgres = '100.125.240.3')
  Sys.setenv(dbname_postgres = 'ddl-dcerm-db')
  Sys.setenv(port_postgres = '5432')
  Sys.setenv(user_postgres = 'ddl_dcerm_app')
  Sys.setenv(password_postgres = 'dcerm_app')
}

#' load_environment_local
#' @description Crea le variabili d'ambiente per per PC locale
#' @export
load_environment_local <- function(){
  Sys.setenv(host_postgres = '10.218.200.90')
  Sys.setenv(dbname_postgres = 'ddl-dcerm-db')
  Sys.setenv(port_postgres = '5432')
  Sys.setenv(user_postgres = 'ddl_dcerm_app')
  Sys.setenv(password_postgres = 'dcerm_app')
}