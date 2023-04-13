#' tbl.list
#' @description Crea un metodo \link[dplyr]{tbl} per la classe `list` che 
#' restituisce un `tibble` da una lista di `tibble`
#' @param src `list` 
#' @param df_name `chr`
#' @return `tibble`
#' @export

tbl.list <- function(src, df_name){
  src[[df_name]]
}