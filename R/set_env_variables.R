#' carica_env_vars
#' @description Crea le variabili d'ambiente per VM e Airflow se `.link_postgres_locale == "NO"`
#' o per PC locale se `.link_postgres_locale == "SI"`
#' @param .link_postgres_locale  `chr` (SI/NO)
#' @return `NULL`
#' @export
carica_env_vars <- function(.link_postgres_locale){
  
  if(.link_postgres_locale == "SI"){
    Sys.setenv(host_postgres = '10.218.200.90')
    Sys.setenv(dbname_postgres = 'ddl-dcerm-db')
    Sys.setenv(port_postgres = '5432')
    Sys.setenv(user_postgres = 'ddl_dcerm_app')
    Sys.setenv(password_postgres = 'dcerm_app')
  } else {
    Sys.setenv(host_postgres = '100.125.240.3')
    Sys.setenv(dbname_postgres = 'ddl-dcerm-db')
    Sys.setenv(port_postgres = '5432')
    Sys.setenv(user_postgres = 'ddl_dcerm_app')
    Sys.setenv(password_postgres = 'dcerm_app')
  }

}