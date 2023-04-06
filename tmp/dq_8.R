"SELECT  COMPLETO.output1, RISULTATO.conteggio FROM  (
  SELECT COUNT(1) as conteggio,   1 as riferimento FROM   `{PROJECT_ID}.{DATASET_ECAP}.{SHIFT_SENSITIVITY_BASE}` 
  WHERE DAT_REPORT ='{dat_report}'   AND ID_VERSIONE = {versione_shift_sensitivity_base} ) RISULTATO 
  LEFT JOIN (
    SELECT DISTINCT A.DES_SHOCK AS output1,   1 as riferimento FROM (   
      SELECT     DES_SHOCK FROM     `{PROJECT_ID}.{DATASET_ECAP}.{SHIFT_SENSITIVITY_BASE}` 
      WHERE     DAT_REPORT ='{dat_report}'     AND ID_VERSIONE = {versione_shift_sensitivity_base} ) A 
    LEFT JOIN (   
      SELECT     DES_SHOCK_SHIFT_SENSITIVITY 
      FROM     `{PROJECT_ID}.{DATASET_ECAP}.{MAPPING_DES_SHOCK}` 
      WHERE     DAT_REPORT ='{dat_report}'     AND ID_VERSIONE ={versione_mapping_des_shock} ) B 
    ON   A.DES_SHOCK = B.DES_SHOCK_SHIFT_SENSITIVITY 
    WHERE   B.DES_SHOCK_SHIFT_SENSITIVITY IS NULL )
  COMPLETO ON   (COMPLETO.riferimento = RISULTATO.riferimento) "

dq_8 <- function(.con) {
  
  # estrae tabelle per dq
  shift_sensitivity_base <- tbl(.con, "TE_IRRBB_SHIFT_SENSITIVITY_BASE")
  mapping_des_shock <- tbl(.con, "TE_IRRBB_MAPPING_DES_SHOCK")
  
  # crea tabella df_errors
  df1 <-  shift_sensitivity_base %>% 
    filter(DAT_REPORT == dat_report & 
             ID_VERSIONE == versione_shift_sensitivity_base) %>% 
    summarise(conteggio = n(),
              riferimento = 1)
  
  df2 <-  shift_sensitivity_base %>% 
    filter(DAT_REPORT == dat_report & 
             ID_VERSIONE == versione_shift_sensitivity_base) %>% 
    select(DES_SHOCK) %>% 
    distinct() %>% 
    mutate(riferimento = 1,
           output1 = DES_SHOCK)
  
  df3 <- mapping_des_shock %>% 
    filter(DAT_REPORT == dat_report & 
             ID_VERSIONE == versione_mapping_des_shock) %>% 
    select(DES_SHOCK_SHIFT_SENSITIVITY)
    
  df4 <- df2 %>% 
    left_join(df3, by = c("DES_SHOCK" = "DES_SHOCK_SHIFT_SENSITIVITY")) %>% 
    filter(is.na(DES_SHOCK))
  
  
  df_errors <- df1 %>% 
    left_join(df4, by = "riferimento") %>% 
    select(output1, conteggio) %>% 
    collect()
  
  return(df_errors)
}
