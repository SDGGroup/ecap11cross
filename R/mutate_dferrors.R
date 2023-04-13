#' mutate_dferrors
#' @description Modifica `df_errors` per la scrittura su Postgres
#' @param .df_errors `tibble`:
#' * `output1` `int`
#' * `output2` `int` opzionale
#' * `output3` `int` opzionale
#' * `conteggio` `int`
#' @inheritParams eval_dferrors
# #' @param .cod_processo (inherited from eval_dferrors)
# #' @param .cod_severita  (inherited from eval_dferrors)
# #' @param .cod_controllo  (inherited from eval_dferrors)
# #' @param .des_controllo  (inherited from eval_dferrors)
# #' @param .msg  (inherited from eval_dferrors)
# #' @param .params_config (inherited from eval_dferrors)
#' @returns `tibble`:
#' * `COD_PROCESSO` `chr`
#' * `COD_SEVERITA` `chr`
#' * `COD_CONTROLLO` `chr`
#' * `DES_CONTROLLO` `chr`
#' * `DAT_REPORT` `date (%Y-%m-%d)`
#' * `ID_VERSIONE` `int`
#' * `DAT_INSERIMENTO` `date (%Y-%m-%d)`
#' @export

mutate_dferrors <- function(.df_errors, 
                            .cod_processo, 
                            .cod_severita, 
                            .cod_controllo, 
                            .des_controllo,
                            .msg,
                            .params_config){
  
  df_errors <- .df_errors %>%
    mutate(COD_PROCESSO    = .cod_processo,
           COD_SEVERITA    = .cod_severita,
           COD_CONTROLLO   = .cod_controllo,
           DES_CONTROLLO   = .des_controllo,
           DAT_REPORT      = dat_report,
           ID_VERSIONE     = as.integer(out_version),
           DAT_INSERIMENTO = Sys.Date(),
    ) 
  
  df_errors <- df_errors %>% 
    mutate(DES_ESITO = as.character(glue(.msg, .envir =  environment())))
  
  # eliminiamo se ci sono, le colonne che non sono richieste dalla tabella TE_DIAGNOSTICA
  df_errors <- df_errors %>% 
    select(-starts_with("output"))
  
  return(df_errors)
}