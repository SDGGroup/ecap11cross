#' retrieve_params
#' @description Recupera dei parametri necessari all'esecuzione del processo
#' @param .con  .con un oggetto output di \link{connect2postgres}, usato per comunicare con il database 
#' @param .id_elaborazione `int` id elaborazione riferito al singolo processo    
#' @param .dataset `chr` nome del dataset
#' @returns `tibble` con il risultato della query, da interrogare con `[param] = value`
#' @export
#' 
retrieve_params <- function(.con, .id_elaborazione){
  
  params <- query_postgres(.con = .con,
                           .query = glue("select * from get_param_all_version({.id_elaborazione})"))
  return(tibble(params))
  
}