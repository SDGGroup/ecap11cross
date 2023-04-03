# funzione per connettersi a big query
connect2bq <- function(.project_id, .dataset){
  
  # autenticazzione a big query
  bqr_auth()
  
  # connessione a big query
  con <- dbConnect(
    bigrquery::bigquery(),
    project = .project_id,
    dataset = .dataset
  )
  
  return(con)
}

# funzione per connettersi a postgres
connect2postgres <- function(){
  
  con <- dbConnect(
    RPostgres::Postgres(), 
    host = '100.125.240.3',
    port = '5432',
    dbname ='ddl-dcerm-db',
    user = "ddl_dcerm_app",
    password = "dcerm_app"
  )
  
  return(con)
}

# funzione per scrivere df su bq
writedf2bq <- function(.project_id, 
                       .dataframe, 
                       .dataset, 
                       .tabella, 
                       .out_version,
                       .dat_report,
                       .tab_schema = NULL){
  
  dataframe <- .dataframe %>% 
    mutate(ID_VERSIONE = as.integer(.out_version),
           DAT_REPORT = as.Date(.dat_report)
    )
  
  if(is.null(.tab_schema)){
    tab_schema <- schema_fields(dataframe)
  }
  
  # Questo non è un errore vero:
  # i 2023-03-31 11:15:13 > Request Status Code:  404
  # Error: API returned
  # il df viene scritto correttamente 
  bqr_upload_data(projectId         = .project_id,
                  datasetId         = .dataset,
                  tableId           = .tabella,
                  upload_data       = dataframe,
                  create            = 'CREATE_IF_NEEDED',
                  writeDisposition  = 'WRITE_APPEND',
                  wait              = FALSE,
                  schema            = tab_schema)
  
}

writedf2postgres <- function(.nome_tabella, .oggetto){
  
  con <- connect2postgres()
  result <- dbWriteTable(con, .nome_tabella, .oggetto, append = TRUE)
  dbDisconnect(con)
  
}


# funzione che modifica df_errors per avere la struttura della TA_DIAGNOSTICA
mutate_dferrors <- function(.df_errors, 
                            .dat_report,
                            .out_version,
                            .cod_processo, 
                            .cod_severita, 
                            .cod_controllo, 
                            .des_controllo,
                            .msg){
  
  df_errors <- .df_errors %>%
    mutate(COD_PROCESSO    = .cod_processo,
           COD_SEVERITA    = .cod_severita,
           COD_CONTROLLO   = .cod_controllo,
           DES_CONTROLLO   = .des_controllo,
           DAT_REPORT      = .dat_report,
           ID_VERSIONE     = as.integer(.out_version),
           DAT_INSERIMENTO = Sys.Date(),
    ) 
  
  # sostituisce espressioni tra {} in DES_ESITO = .msg con glue
  # la priorit? ? data alle variabili della tibble e se non vengono trovate
  # usa le variabili nell'ambiente 
  df_errors <- df_errors %>% 
    mutate(DES_ESITO = as.character(glue(.msg)))
  
  # TODO: perche' avevano messo con sapply?
  # df_errors$DES_ESITO <- sapply(as.character(df_errors$DES_ESITO)  , function (x) glue(x))
  
  # eliminiamo se ci sono, le colonne che non sono richieste dalla tabella TE_DIAGNOSTICA
  df_errors <- df_errors %>% 
    select(-starts_with("output"))
  
  return(df_errors)
}