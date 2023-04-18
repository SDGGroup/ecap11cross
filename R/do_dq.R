#' do_dq
#' @description 
#' Esegue i data quality presenti nel catalog 
#' @param .catalog `tibble` contiene tutti controlli del processo:
#' * `cod_controllo` `chr`
#' * `cod_processo` `chr`
#' * `cod_tipo_controllo` `int`
#' * `cod_freq_controllo` `chr`
#' * `cod_severita` `chr`
#' * `des_controllo` `chr`
#' * `msg` `chr`
#' * `stmt_controllo` `chr`
#' * `flg_attivo` `chr`
#' * `tms_creazione` ?
#' * `tms_modifica` ?
#' * `dat_fine_validita` ?
#' * `cod_utente_modifica` ?
#' * `cod_utente_creazione` ?
#' * `nome_funzione` `chr`
#' * `check_interfaccia` `chr`
#' @param .con `S4 object` usato per comunicare con il database Big Query o una lista
#' di `tibble` nella fase di esecuzione delle funzioni contenute in `nome_funzione`
#' @param .con_bigquery `S4 object` usato per comunicare con il database Big Query 
#' @param .con_postgres `S4 object` usato per comunicare con il database Postgres 
#' @param .params_config `list` contiene i parametri aggiuntivi necessari per eseguire
#' le funzioni di data quality 
#' @returns  `tibble`:
#' * `cod_processo` `chr`
#' * `cod_controllo` `chr`
#' * `cod_severita` `int`
#' * `esito` `chr`
#' * `des_controllo` `chr`
#' @export
 
do_dq <- function(.catalog, .con, .con_bigquery, .con_postgres, .params_config){
  
  # creazione df_errors
  catalog <- .catalog %>% 
    filter(check_interfaccia == "NO") %>% 
    mutate(df_errors = invoke_map(nome_funzione, .con = .con, .params_config = .params_config))
  
  catalog_interfaccia <- .catalog %>% 
    filter(check_interfaccia == "SI") %>% 
    mutate(df_errors = invoke_map(nome_funzione, .con = .con))
  
  catalog <- catalog %>% 
    bind_rows(catalog_interfaccia)
  
  # scrittura su big query e/o postgres
  df_esito <- catalog %>% 
    rowwise() %>% 
    mutate(esito = eval_dferrors(df_errors, 
                                 cod_processo,
                                 cod_severita, 
                                 cod_controllo, 
                                 des_controllo, 
                                 msg,
                                 .con_bigquery, 
                                 .con_postgres,
                                 .params_config)
    ) %>% 
    select(cod_processo,
           cod_controllo,
           cod_severita,
           esito)
    
  return(df_esito)
    
}
