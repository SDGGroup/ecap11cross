#' do_DQ.R
#' @description Esegue il controllo corrispondente a `COD_CONTROLLO`.
#' Ci sono due possibilità:
#' * controllo tramite query. `catalog` deve avere popolato il campo `STMT_CONTROLLO` 
#'   con la query che verrà utilizzata per il controllo e `df_errors=NULL`.
#' * controllo diretto se `df_error` è passato come argomento.
#' @param check chr, codice controllo da eseguire.
#' @param catalog tibble, contiene tutti controlli del processo.
#' @param project_id chr, nome del progetto GCP
#' @param out_version int, versione dell'output
#' @param df_error tibble con almeno 1 colonna: !!! TODO: aggiungere tipi !!!
#' * OUTPUT1,
#' * OUTPUT2 opzionale,
#' * OUTPUT3 opzionale,
#' * CONTEGGIO opzionale.
#' @param y A number.
#' @returns Un booleano. `TRUE` in presenza di errore nel controllo.
#' @export

do_DQ <- function(check, catalog, project_id, out_version, df_errors=NULL) {

  out2log("\n Diagnostico ",check," in esecuzione...")

  B          <- FALSE
  check_row  <- catalog %>% filter(COD_CONTROLLO==check)
  tms_inizio <- Sys.time()

  # Se df_error non è fornito in input lo creiamo eseguendo una query su BQ
  # Questa query è definita nel campo STMT_CONTROLLO del catalog.

  if (is.null(df_errors)) {

    out2log('\n Controllo da query \n')
    qry       <- glue(check_row$STMT_CONTROLLO)
    out2log("\n ",qry)

    temp      <- bq_project_query(project_id, qry)
    df_errors <- bq_table_do
    wnload(temp)
  }

  # Casistica in cui non ho il conteggio (controllo di data quality ERROR)
  # se il controllo è andato bene non ho righe,
  # ma non riesco ad utilizzare il resto del codice perchè non aggiunge la colonna conteggio
  # quindi la aggiungiamo in maniera fittizia

  if (check_row$COD_SEVERITA %in% c('ERROR') && nrow(df_errors) == 0) {
    df_errors <- data.frame()
    df_errors <- data.frame(output1 = c(NA), conteggio = c(1))
  }

  conteggio_record_ko <- 0
  conteggio_record    <- unique(df_errors$conteggio)
  df_errors$conteggio <- NULL

  # Variabili di controllo esito scrittura su DataBase
  var_error <- FALSE     # Per BQ TA_DIAGNOSTICA
  uscita    <- FALSE     # Per Postgres te_esito_controllo_dq

  if (!(is.na(df_errors$output1)[1]))
  {

    B <- TRUE

    # df_errors viene modificata per avere la struttura della TA_DIAGNOSTICA
    conteggio_record_ko       <- nrow(df_errors)
    df_errors$COD_PROCESSO    <- check_row$COD_PROCESSO
    df_errors$DAT_REPORT      <- dat_report
    df_errors$ID_VERSIONE     <- as.integer(out_version)
    df_errors$COD_SEVERITA    <- check_row$COD_SEVERITA
    df_errors$COD_CONTROLLO	  <- check_row$COD_CONTROLLO
    df_errors$DES_CONTROLLO	  <- check_row$DES_CONTROLLO
    df_errors$DES_ESITO       <- check_row$MSG
    df_errors$DAT_INSERIMENTO <- Sys.Date()

    errori <- colnames(df_errors)
    if ("output1" %in% errori)
    {
      df_errors$DES_ESITO <- str_replace_all(df_errors$DES_ESITO, '\\{output1\\}',
                                             as.character(df_errors$output1))
    }

    if ("output2" %in% errori)
    {
      df_errors$DES_ESITO <- str_replace_all(df_errors$DES_ESITO, '\\{output2\\}',
                                             as.character(df_errors$output2))
    }

    if ("output3" %in% errori)
    {
      df_errors$DES_ESITO <- str_replace_all(df_errors$DES_ESITO, '\\{output3\\}',
                                             as.character(df_errors$output3))
    }


    df_errors <- as.data.frame(df_errors)

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
    if (check_row$COD_SEVERITA %in% c('WARNING'))
    {
      # crea il dataframe vuoto da scrivere su Postgres
      df_postgres <- data.frame(matrix(ncol = 12,nrow = 1,
                                       dimnames=list(NULL, c("dat_report", "num_versione", "cod_processo",
                                                             "cod_controllo","des_controllo","cod_esito",
                                                             "dat_inserimento","num_tot_casi","num_tot_casi_warn",
                                                             "num_tot_casi_error","tms_inizio","tms_fine")))
      )
      # popola il dataframe da scrivere su Postgres

      df_postgres$dat_report          <- dat_report
      df_postgres$num_versione        <- as.integer(out_version)
      df_postgres$cod_processo        <- check_row$COD_PROCESSO
      df_postgres$cod_controllo       <- check_row$COD_CONTROLLO
      df_postgres$des_controllo       <- check_row$DES_CONTROLLO
      df_postgres$cod_esito           <- (if(var_error){'ERROR'} else if(B){'KO'} else {'OK'})
      df_postgres$dat_inserimento	    <- dat_report
      df_postgres$num_tot_casi	      <- conteggio_record
      df_postgres$num_tot_casi_warn	  <- (if(check_row$COD_SEVERITA == 'ERROR'){0} else {conteggio_record_ko})
      df_postgres$num_tot_casi_error	<- (if(check_row$COD_SEVERITA == 'ERROR'){conteggio_record_ko} else {0})
      df_postgres$tms_inizio	        <- tms_inizio
      df_postgres$tms_fine	          <- tms_fine

      # scrive il dataframe su Postgres
      tryCatch({write_df_on_postgres(nome_tabella = 'te_esito_controllo_dq',
                                     oggetto = df_postgres)},
               error = function(e) {uscita <<- TRUE})
    }

    out2log("\n Diagnostico ",check," eseguito\n\n")

  }

  if(var_error){
    out2log("\n Errore nella scrittura dell'esito controllo ", check, " in TA_DIAGNOSTICA \n")
  }

  if(uscita){
    out2log("\n Errore nella scrittura dell'esito controllo ", check, " in te_esito_controllo_dq \n")
  }

  return(B)

}
