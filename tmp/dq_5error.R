dq_5error <- function(.con) {
  
  # estrae tabelle per dq
  shift_sensitivity <- tbl(.con, "TE_IRRBB_SHIFT_SENSITIVITY")
  
  # crea tabella df_errors
  df1 <-  shift_sensitivity %>% 
    filter(DAT_REPORT == '2022-12-31' & 
             ID_VERSIONE == 0) %>% 
    summarise(conteggio = n(),
              riferimento = 1)
  
  df2 <- shift_sensitivity %>% 
    filter(DAT_REPORT == '2022-12-31' & 
             ID_VERSIONE == 0 & 
             COD_PERIMETRO == 'NO_PATH_DEPENDENT') %>% 
    distinct(DES_SHOCK) %>% 
    summarise(output1 = n(),
              riferimento = 1) %>% 
    filter(output1 >= 1)
  
  df_errors <- df1 %>% 
    left_join(df2, by = "riferimento") %>% 
    select(output1, conteggio) %>% 
    collect() %>% 
    bind_rows(
      tibble(output1 = 2,
             conteggio = 123)
    ) %>% 
    mutate(output2 = 2)
  
  return(df_errors)
}