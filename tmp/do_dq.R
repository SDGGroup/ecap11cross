# TODO: valutare se suddividere DQ in più parti 
do_dq <- function(.dq, # funzione o df_errors
                  .cod_processo, 
                  .cod_severita, 
                  .cod_controllo, 
                  .des_controllo, 
                  .msg,
                  .con, 
                  .params_config){
  
  message("Diagnostico ", .cod_controllo, " in esecuzione")
  
  # assegna variabili a parametri nella lista 
  list2env(.params_config, envir = environment())
  
  # esito DQ, B == TRUE ---> errore DQ
  tms_inizio <- Sys.time()
  cod_esito <- "OK"
  
  # R un linguaggio lexically scoped, quando una funzione interna f2 (.dq)
  # a un'altra funzione f1 (do_dq) è chiamata, non cerca le variabili dentro env di f1
  # resettiamo l'env di .dq così è come se .dq fosse definita dentro do_dq
  environment(.dq) <- environment()
  # estrae df_error 
  if(class(.dq) == "function"){
    df_errors <- .dq(.con)
  } else {
    df_errors <- .dq
  }
  
  # salva conteggio ed elimina dal df_errors
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
  
  # conteggio errori 
  conteggio_record_ko <- df_errors %>%
    filter(!is.na(output1)) %>% 
    count() %>% 
    pull(n)

  # first_output != NA -----> DQ fallito
  if (!(is.na(first_output))) {

    cod_esito <- "KO"

    # prepara df_error per scrittura
    environment(mutate_dferrors) <- environment()
    df_errors <- mutate_dferrors(df_errors,
                                 .cod_processo,
                                 .cod_severita,
                                 .cod_controllo,
                                 .des_controllo,
                                 .msg)

    # scrive gli esiti dei controlli su BigQuery, tabella TE_DIAGNOSTICA
    tryCatch({writedf2bq(project_id,
                         df_errors,
                         .dataset = "ds_ddl_general",
                         .tabella = "TE_DIAGNOSTICA",
                         out_version,
                         dat_report)},
             error = function(e) {
               message("Errore nella scrittura dell'esito controllo ", .cod_controllo, " in TE_DIAGNOSTICA")
             })
  }
  
  tms_fine <- Sys.time()

  # in caso di cod_severita = WARNING viene aggiornata anche la tabella Postgres
  if (.cod_severita == "WARNING") {

    # crea il dataframe da scrivere su Postgres
    df_postgres <- tibble(dat_report =  dat_report,
                          num_versione = out_version,
                          cod_processo = .cod_processo,
                          cod_controllo = .cod_controllo,
                          des_controllo =  .des_controllo,
                          cod_esito = cod_esito,
                          dat_inserimento = dat_report,
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
  
  return(cod_esito)
}
