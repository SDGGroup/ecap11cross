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
    # aggiunto conteggio per allinearlo con data quality precedenti
    # TODO: ripensare struttura output funzioni check interfaccia 
    mutate(conteggio =  NA_real_) %>% 
    add_row(tibble(output1 = NA, output2 = NA, conteggio = nrow(shift_sensitivity)))
  
  return(df_errors)
}
