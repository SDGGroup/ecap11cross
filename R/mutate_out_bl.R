#' mutate_out_bl
#' @description Aggiunge le colonne `DAT_REPORT` e `ID_VERSIONE` ai `df_name`
#' presenti in `df_list`
#' @param .df_list `named list` contiene i data frame 
#' @param .df_name `vector (chr)` nomi dei data frame
#' @param .out_version `int`
#' @param .dat_report `date (%Y-%m-%d)`
#' @return `list` contiene i data frame 
#' @export

mutate_out_bl <- function(.df_list, .df_name, .out_version, .dat_report){
  
  .df_list %>% 
    map_at(.at = .df_name,
           .f  = ~ .x %>% mutate(ID_VERSIONE = .out_version, 
                                 DAT_REPORT = .dat_report))
}