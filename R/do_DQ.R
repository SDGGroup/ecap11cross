#' do_DQ
#' @description Esegue il controllo corrispondente a `COD_CONTROLLO`. Ci sono due possibilità:
#' * controllo tramite query. `catalog` deve avere popolato il campo `STMT_CONTROLLO`
#'   con la query che verrà utilizzata per il controllo e `df_errors=NULL`
#' * controllo diretto se `df_error` è passato come argomento
#' @param check `chr` codice controllo da eseguire
#' @param catalog `tibble` contiene tutti controlli del processo:
#' * tba
#' * tba
#' * tba
#' @param project_id `chr` nome del progetto GCP
#' @param out_version `int` versione dell'output
#' @param df_error `tibble`:
#' * OUTPUT1 ?,
#' * OUTPUT2 ? opzionale,
#' * OUTPUT3 ? opzionale,
#' * CONTEGGIO ? opzionale.
#' @returns `bool`  `TRUE` in presenza di errore nel controllo
#' @export

do_DQ <- function(check, catalog, project_id, out_version, df_errors = NULL) {

  #TODO: aggiungere tipi e colonne di df_error nella documentazione
  #TODO: aggiungere colonne e tipi di catalog
  out2log("\n Diagnostico ",check," in esecuzione...")

  B          <- FALSE
  check_row  <- catalog %>% filter(COD_CONTROLLO==check)
  tms_inizio <- Sys.time()

  # Se df_error non è fornito in input lo creiamo eseguendo una query su BQ
  # Questa query è definita nel campo STMT_CONTROLLO del catalog.

  if (is.null(df_errors)) {

    out2log('\n Controllo da query \n')
    qry       <- glue(check_row %>%
                        select(STMT_CONTROLLO) %>%
                        pull())
    out2log("\n ",qry)

    temp      <- bq_project_query(project_id, qry)
    df_errors <- bq_table_download(temp)

  }

  # Casistica in cui non ho il conteggio (controllo di data quality ERROR)
  # se il controllo è andato bene non ho righe,
  # ma non riesco ad utilizzare il resto del codice perchè non aggiunge la colonna conteggio
  # quindi la aggiungiamo in maniera fittizia

  if (check_row$COD_SEVERITA %in% c('ERROR') && nrow(df_errors) == 0) {
    df_errors <- tibble()
    df_errors <- tibble(output1 = NA_character_,
                        conteggio = 1)
  }

  conteggio_record_ko <- 0
  conteggio_record    <- df_errors %>%
    select(conteggio) %>%
    distinct() %>%
    pull()
  df_errors <- df_errors %>%
    mutate(conteggio = NA_integer_)

  # Variabili di controllo esito scrittura su DataBase
  var_error <- FALSE     # Per BQ TA_DIAGNOSTICA
  uscita    <- FALSE     # Per Postgres te_esito_controllo_dq

  first_output1 <- df_errors %>%
    select(output1) %>%
    slice(1) %>%
    pull()

  if (!(is.na(first_output1))) {

    B <- TRUE

    # df_errors viene modificata per avere la struttura della TA_DIAGNOSTICA
    conteggio_record_ko       <- nrow(df_errors)

    df_errors <- df_errors %>%
      bind_cols(
        check_row %>%
          select(COD_PROCESSO,
                 COD_SEVERITA,
                 COD_CONTROLLO,
                 DES_CONTROLLO,
                 MSG)
      ) %>%
      mutate(DAT_REPORT      = dat_report,
             ID_VERSIONE     = as.integer(out_version),
             DAT_INSERIMENTO = Sys.Date(),
             DES_ESITO       = MSG
      )

    errori <- colnames(df_errors)
    if ("output1" %in% errori) {
      df_errors$DES_ESITO <- str_replace_all(df_errors$DES_ESITO, '\\{output1\\}',
                                             as.character(df_errors$output1))
    }

    if ("output2" %in% errori) {
      df_errors$DES_ESITO <- str_replace_all(df_errors$DES_ESITO, '\\{output2\\}',
                                             as.character(df_errors$output2))
    }

    if ("output3" %in% errori) {
      df_errors$DES_ESITO <- str_replace_all(df_errors$DES_ESITO, '\\{output3\\}',
                                             as.character(df_errors$output3))
    }

    df_errors$DES_ESITO <- sapply(as.character(df_errors$DES_ESITO)  , function (x) glue(x))

    #eliminiamo se ci sono, le colonne che non sono richieste dalla tabella TE_DIAGNOSTICA

    df_errors$output1 <- NULL
    df_errors$output2 <- NULL
    df_errors$output3 <- NULL

    # scrive gli esiti dei controlli su BigQuery, tabella TE_DIAGNOSTICA
    tryCatch({write_df_2_BQ(project_id,
                            df_errors,
                            dataset = DATASET_GENERAL,
                            tabella = TAB_DIAGNOSTICA)},
             error = function(e) {var_error <<- TRUE})


    tms_fine <- now()

    # in caso di cod_severita = WARNING viene aggiornata anche la tabella Postgres
    if (check_row$COD_SEVERITA %in% c('WARNING')) {

      # crea il dataframe da scrivere su Postgres
      df_postgres <- tibble(dat_report = dat_report,
                            num_versione = as.integer(out_version),
                            cod_processo = check_row$COD_PROCESSO,
                            cod_controllo = check_row$COD_CONTROLLO,
                            des_controllo =  check_row$DES_CONTROLLO,
                            cod_esito = (if(var_error){'ERROR'} else if(B){'KO'} else {'OK'}),
                            dat_inserimento = dat_report,
                            num_tot_casi = conteggio_record,
                            num_tot_casi_warn =  (if(check_row$COD_SEVERITA == 'ERROR'){0} else {conteggio_record_ko}),
                            num_tot_casi_error = (if(check_row$COD_SEVERITA == 'ERROR'){conteggio_record_ko} else {0}),
                            tms_inizio = tms_inizio,
                            tms_fine = tms_fine
      )

      # scrive il dataframe su Postgres
      tryCatch({write_df_on_postgres(nome_tabella = 'te_esito_controllo_dq',
                                     oggetto = df_postgres)},
               error = function(e) {uscita <<- TRUE})
    }

    out2log("\n Diagnostico ",check," eseguito\n\n")

  }

  if (var_error) {
    out2log("\n Errore nella scrittura dell'esito controllo ", check, " in TA_DIAGNOSTICA \n")
  }

  if (uscita) {
    out2log("\n Errore nella scrittura dell'esito controllo ", check, " in te_esito_controllo_dq \n")
  }

  return(B)

}
