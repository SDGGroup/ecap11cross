# SELECT  COMPLETO.output1, RISULTATO.conteggio FROM  (
#   SELECT COUNT(1) as conteggio,   1 as riferimento FROM  
#   `{PROJECT_ID}.{DATASET_ECAP}.{SHIFT_SENSITIVITY}` 
#   WHERE DAT_REPORT ='{dat_report}'   AND ID_VERSIONE = {versione_shift_sensitivity} ) RISULTATO 
# LEFT JOIN (
#   SELECT count(DES_SHOCK) AS output1,  1 as riferimento FROM (   
#      SELECT     distinct DES_SHOCK as  DES_SHOCK FROM     `{PROJECT_ID}.{DATASET_ECAP}.{SHIFT_SENSITIVITY}` 
#      WHERE DAT_REPORT ='{dat_report}'   AND ID_VERSIONE = {versione_shift_sensitivity} and COD_PERIMETRO = 'NO_PATH_DEPENDENT')
#   HAVING count(DES_SHOCK)>=1 ) COMPLETO 
# ON (COMPLETO.riferimento = RISULTATO.riferimento)

dq_5 <- function(.con) {
  
  # estrae tabelle per dq
  shift_sensitivity <- tbl(.con, "TE_IRRBB_SHIFT_SENSITIVITY")
  
  # crea tabella df_errors
  df1 <-  shift_sensitivity %>% 
    filter(DAT_REPORT == dat_report & 
             ID_VERSIONE == versione_shift_sensitivity) %>% 
    summarise(conteggio = n(),
              riferimento = 1)
  
  df2 <- shift_sensitivity %>% 
    filter(DAT_REPORT == dat_report & 
             ID_VERSIONE == 0 & 
             COD_PERIMETRO == 'NO_PATH_DEPENDENT') %>% 
    distinct(DES_SHOCK) %>% 
    summarise(output1 = n(),
              riferimento = 1) %>% 
    filter(output1 > 1)
  
  df_errors <- df1 %>% 
    left_join(df2, by = "riferimento") %>% 
    select(output1, conteggio) %>% 
    collect()
  
  return(df_errors)
}
