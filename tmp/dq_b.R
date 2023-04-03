dq_b <- function(.con) {
  
  # estrae tabelle per dq
  shift_sensitivity <- tbl(.con, "TE_IRRBB_SHIFT_SENSITIVITY")
  
  shift_sensitivity <- shift_sensitivity %>% 
    collect() %>% 
    mutate(CHIAVE = paste(COD_PERIMETRO,
                          DES_SHOCK,
                          COD_VALUTA,
                          COD_ENTITY,
                          ID_MESE_MAT, sep = "|")
    )

  df_errors <- valore_campo_string(df_table = shift_sensitivity,
                                 col_check = 'COD_PERIMETRO',
                                 val_string = c('PATH_DEPENDENT', 'NO_PATH_DEPENDENT')) %>% 
    mutate(conteggio = NA_integer_)
  
  return(df_errors)
}
