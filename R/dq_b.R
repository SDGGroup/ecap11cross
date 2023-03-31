con <- connect2bq(project_id, dataset)

dq_b <- function(con){
  
  # estrae tabelle per dq
  shift_sensitivity <- tbl(con, "TE_IRRBB_SHIFT_SENSITIVITY")
  
  
}

df_shift_sensitivity$CHIAVE <- paste(df_shift_sensitivity$COD_PERIMETRO,
                                     df_shift_sensitivity$DES_SHOCK,
                                     df_shift_sensitivity$COD_VALUTA,
                                     df_shift_sensitivity$COD_ENTITY,
                                     df_shift_sensitivity$ID_MESE_MAT, sep = "|" )


check_b <- valore_campo_string(df_table = df_shift_sensitivity,
                               col_check = 'COD_PERIMETRO',
                               val_string = c('PATH_DEPENDENT', 'NO_PATH_DEPENDENT'))