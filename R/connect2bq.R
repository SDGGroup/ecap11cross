#' connect2bq
#' @description Crea una connessione a Big Query
#' @param .project_id `chr` nome del progetto
#' @param .dataset `chr` nome del dataset
#' @returns Un S4 object ereditato da \link[DBI]{dbConnect}, usato per comunicare con il database
#' @export

connect2bq <- function(.project_id, .dataset){
  
  # autenticazzione a big query
  bqr_auth()
  
  # connessione a big query
  con <- dbConnect(
    bigrquery::bigquery(),
    project = .project_id,
    dataset = .dataset
  )
  
  return(con)
}
