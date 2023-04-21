#' do_scrittura_output
#' @description Scrive le tabelle risultanti dall'esecuzione della business logic
#' su Big Query
#' @param .df_config `tibble` le cui righe contengono le informazioni per la scrittura
#' di ciascun data frame:
#' * `df_name` `chr`
#' * `df` `list (tibble)`
#' * `project_id` `chr`
#' * `dataset` `chr`
#' * `tabella` `chr`
#' * `campi_required` `chr`
#' @param .con_bigquery `S4 object` usato per comunicare con il database Big Query 
#' @param .out_version `int`
#' @param .dat_report `date (%Y-%m-%d)`
#' @return `vector (boolean)` indica per ciascun dataframe se la scrittura è andata a 
#' buon fine (`TRUE`)
#' @export

do_scrittura_output <- function(.df_config, .con_bigquery, .out_version, .dat_report){
  
  # aggiunge le colonne DAT_REPORT e ID_VERSIONE perché servono nello schema per essere dichiarate REQUIRED
  # solo alle tabelle che hanno dei campi required
  df_config <- .df_config %>%
    rowwise() %>% 
    mutate(df = ifelse(campi_required == "SI",
                       list(df %>% mutate(ID_VERSIONE = .out_version, 
                                          DAT_REPORT = .dat_report)),
                       df) 
    )
  
  # Nel caso in cui ci siano dei campi required è necessario aggiornare manualmente
  # il "mode" dello schema.
  # STEP 1: creare lo schema tramite la funzione schema_fields
  schema <- df_config$df %>% 
    map(schema_fields)
  
  # estrae schema da big query
  query <- "SELECT ordinal_position, column_name, data_type, is_nullable 
  FROM `{project_id}.{dataset}.INFORMATION_SCHEMA.COLUMNS` 
  WHERE 1=1 and table_catalog = '{project_id}' 
  and table_schema = '{dataset}' 
  and table_name = '{tabella}' order by ordinal_position"
  
  # sostituisce nomi tra {}
  query <- df_config %>% 
    mutate(query = glue(query)) %>% 
    pull(query)
  
  schema_bq <- query %>% 
    map(do_query, .con = .con_bigquery)
  names(schema_bq) <- df_config$df_name
  
  # STEP 2: aggiornare il mode = "REQUIRED" delle colonne required, basandosi
  # sullo schema precedentemente estratto
  for (i in 1:length(schema)) {
    el_schema <- schema[[i]]
    el_schema_bq <-  schema_bq[[i]]
    el_schema <- el_schema %>% 
      map_if(.p = ~ (el_schema_bq %>% 
                       filter(column_name == .x$name) %>% 
                       pull(is_nullable)) == "NO",
             .f = ~ c(.x, mode = "REQUIRED"))
    schema[[i]] <- el_schema
  }
  
  # aggiunge schema a df_config
  df_schema <- tibble(df_name = df_config$df_name,
                      schema = schema)
  df_config <- df_config %>% 
    left_join(df_schema, by = "df_name")
  
  # scrive su big query
  res <- df_config %>% 
    rowwise() %>% 
    summarise(writedf2bq(.project_id  = project_id, 
                         .dataframe   = df, 
                         .dataset     = dataset, 
                         .tabella     = tabella, 
                         .out_version = .out_version,
                         .dat_report  = .dat_report,
                         .tab_schema  = schema), .groups = "drop") %>% 
    pull()
  
  return(res)
}
