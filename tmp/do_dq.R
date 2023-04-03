# df_errrors è una funzione per estrarre il df o il df stesso
do_dq <- function(.df_errors,
                  .project_id, 
                  .dataset,
                  .dat_report,
                  .out_version,
                  .cod_processo, 
                  .cod_severita, 
                  .cod_controllo, 
                  .des_controllo, 
                  .msg,
                  .dataset_general,
                  .tab_diagnostica){
  
  message("Diagnostico ", .cod_controllo, " in esecuzione")
  
  # Variabili di controllo esito scrittura su DataBase
  var_error <- FALSE     # Per BQ TA_DIAGNOSTICA
  uscita    <- FALSE     # Per Postgres te_esito_controllo_dq
  
  # esito DQ, B == F ---> DQ passato
  tms_inizio <- Sys.time()
  B <- FALSE
  
  # estrae df_error se non dato in input
  if (class(.df_errors) == "function") {
    # connect to bq
    con <- connect2bq(.project_id, .dataset)
    df_errors <- .df_errors(con)
  } 
  
  # salva conteggio ed elimina dal df_errors
  conteggio_record_ko <- 0
  conteggio_record <- df_errors %>%
    select(conteggio) %>%
    distinct() %>%
    pull()
  
  df_errors <- df_errors %>%
    select(-conteggio)
  
  first_output <- df_errors %>%
    select(output1) %>% 
    slice(1) %>%
    pull()
  
  # first_output != NA -----> DQ fallito
  if (!(is.na(first_output))) {
    
    B <- TRUE
    
    # prepara df_error per scrittura 
    df_errors <- mutate_dferrors(df_errors, 
                                 .dat_report,
                                 .out_version,
                                 .cod_processo, 
                                 .cod_severita, 
                                 .cod_controllo, 
                                 .des_controllo, 
                                 .msg)
    
    # scrive gli esiti dei controlli su BigQuery, tabella TE_DIAGNOSTICA
    tryCatch({writedf2bq(.project_id, 
                         df_errors,
                         .dataset_general, 
                         .tab_diagnostica,
                         .out_version,
                         .dat_report)},
             error = function(e) {
               var_error <- TRUE
               message("Errore nella scrittura dell'esito controllo ", .cod_controllo, " in ", .tab_diagnostica)
             })
  }
  tms_fine <- Sys.time()
    
  # in caso di cod_severita = WARNING viene aggiornata anche la tabella Postgres
  if (.cod_severita == "WARNING") {
    
    cod_esito <- ifelse(B == T, 'KO', 'OK')
    num_tot_casi_warn <- ifelse(.cod_severita == "ERROR", 0, conteggio_record_ko)
    num_tot_casi_error <- ifelse(.cod_severita == "ERROR", 0, conteggio_record_ko)
    
    # crea il dataframe da scrivere su Postgres
    df_postgres <- tibble(dat_report =  .dat_report,
                          num_versione = .out_version,
                          cod_processo = .cod_processo,
                          cod_controllo = .cod_controllo,
                          des_controllo =  .des_controllo,
                          cod_esito = cod_esito,
                          dat_inserimento = .dat_report,
                          num_tot_casi = conteggio_record,
                          num_tot_casi_warn =  conteggio_record_ko,
                          num_tot_casi_error = 0,
                          tms_inizio = tms_inizio,
                          tms_fine = tms_fine
    )
    
    # scrive il dataframe su Postgres
    tryCatch({writedf2postgres(.nome_tabella = 'te_esito_controllo_dq',
                               .oggetto = df_postgres)},
             error = function(e) {
               message("Errore nella scrittura dell'esito controllo ", .cod_controllo, " in te_esito_controllo_dq")
               })
    
  }

  message("Diagnostico ", .cod_controllo, " eseguito")
  return(B)
}
