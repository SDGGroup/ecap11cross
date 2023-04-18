#' assign_params
#' @description Assegna a nuove variabili in `env` gli elementi
#' con nome `.par_names` presenti nella lista `.list`
#' @param .list `named list`
#' @param .par_names `vector (chr)`
#' @param .env un \link{envir} o `NULL` 
#' @return `NULL`
#' @export

assign_params <- function(.list, .par_names, .env){
  
  list_sub <- .list[.par_names]
  list2env(list_sub, envir = .env)
  
}
