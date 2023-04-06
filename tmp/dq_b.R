#' valore_campo_string <-  function(df_table,col_check, val_string)
#' {
#'   # Eseguo un controllo sul campo stringa "VALORE" 
#'   #  Args:
#'   #'    @df_table   (dataframe)          : dataframe da controllare (ci deve essere il campo CHIAVE 
#'   #'                                       con la chiave da vedere in output)
#'   #'    @col_check  (character)          : colonna da controllare.
#'   #'                                       In questo controllo abbiamo col_check='COD_LT_ST'.
#'   #'    @val_string (vettore di stringhe): vettore contenete la lista delle stringhe che il campo "COL_CHECK" può assumere. 
#'   #'                                    
#'   #  Returns:
#'   #'    @df_diagn   (dataframe)          : dataframe in cui è riportato il diagnostico
#'   
#'   #rinomino "col_check" ovvero la colonna COD_CORRETTIVI con la stringa VALORE
#'   names(df_table)[names(df_table)==col_check] <- 'VALORE'
#'   
#'   # Check: vengono salvate in df_errors la chiave ed il valore dei cod_macroaggregato il cui
#'   # VALORE non è in val_string.
#'   df_errors <- df_table %>% filter( !(VALORE %in% val_string)) %>% select (CHIAVE,VALORE)
#'   
#'   # Creazione della concatenazione delle chiavi al fine di creare {error} il diagnostico 
#'   df_diagn <- df_errors
#'   colnames(df_diagn) <- c('output1','output2')
#'   return(as.data.frame(df_diagn))
#' }


dq_b <- function(.con) {
  
  val_string = c('PATH_DEPENDENT', 'NO_PATH_DEPENDENT')
  
  # estrae tabelle per dq
  shift_sensitivity <- tbl(.con, "TE_IRRBB_SHIFT_SENSITIVITY")
  
  shift_sensitivity <- shift_sensitivity %>% 
    mutate(CHIAVE = paste0(COD_PERIMETRO,
                          DES_SHOCK,
                          COD_VALUTA,
                          COD_ENTITY,
                          ID_MESE_MAT)
    ) %>% 
    #rinomino "col_check" ovvero la colonna COD_CORRETTIVI con la stringa VALORE
    rename(VALORE = COD_PERIMETRO) %>% 
    # Check: vengono salvate in df_errors la chiave ed il valore dei cod_macroaggregato il cui
    #'   # VALORE non è in val_string.
    filter(!(VALORE %in% val_string)) %>% 
    select(CHIAVE,VALORE) %>% 
    collect()
  
  # Creazione della concatenazione delle chiavi al fine di creare {error} il diagnostico 
  df_errors <- shift_sensitivity %>% 
    rename(output1 = CHIAVE,
           output2 = VALORE) %>% 
    # aggiunto conteggio per allinearlo con data quality precedenti
    # TODO: ripensare struttura output funzioni check interfaccia 
    mutate(conteggio =  NA_real_) %>% 
    add_row(tibble(output1 = NA, output2 = NA, conteggio = nrow(shift_sensitivity))) 
    
  
  return(df_errors)
}
