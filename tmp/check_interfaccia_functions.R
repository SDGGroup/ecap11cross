controllo_chiave  <- function(df,chiave)
{
  # Funzione per controllare eventuali duplicati di chiave.
  #    Args:
  #'     @df       (dataframe)           : dataframe sul quale eseguire il controllo
  #'     @chiave   (vettore di stringhe) : vettore di campi considerati chiave
  #    Returns:
  #'     @df.diagn (dataframe)           : dataframe in cui é riportato il diagnostico. 
  
  # conteggio delle osservazioni per la chiave definita
  df_raggruppato <- df %>% 
    group_by(df[,chiave]) %>% 
    count()
  
  # Individuazione dei duplicati
  df_duplicati <- df_raggruppato[which(df_raggruppato$n > 1),]
  
  # Crezione dellla concatenazione delle chiavi al fine di creare {error} il diagnostico
  df_diagn <- df_duplicati %>% unite(error, all_of(chiave), sep = "|", remove =T)
  colnames(df_diagn) <- c('output1', 'output2')
  return(as.data.frame(df_diagn))
}

somma_righe_matrici  <- function(df_matrix)
{
  
  # Funzione per controllare eventuali valori assunti dalla somma delle chiavi diversi da 1.
  #   Args:
  #'     @df_matrix (dataframe) : dataframe con almeno 4 colonne con nomi 'CHIAVE', 'COD_RATING-OUT'
  #                               e 'VAL_*'
  #   Returns:
  #'     @df.diagn  (dataframe) : dataframe in cui é riportato il diagnostico.
  
  # Rinomino la colonna col_check con la stringa valore 'VALORE'
  names(df_matrix)[which(names(df_matrix) %like% 'VAL_')] <- 'VALORE'
  
  # Somma per chiave
  df_errors <- df_matrix %>% group_by(CHIAVE) %>% summarise(VALORE = sum(VALORE))
  
  # Individuazione dei valori diversi da 1
  df_errors <- df_errors[which(df_errors$VALORE > (1 + epsilon) | df_errors$VALORE < (1 - epsilon)),]
  
  # Crezione della concatenazione delle chiavi al fine di creare {error} il diagnostico
  df_diagn <- df_errors
  colnames(df_diagn) <- c('output1','output2')
  return(as.data.frame(df_diagn))
}


matrici_quadrate  <- function(df_matrix)
{
  
  #  Controllo su valori diversi assunti da COD_RATING_IN  e COD_RATING_OUT.
  #   Args:
  #'     @df_matrix (dataframe) : dataframe che deve avere almeno 3 colonne con nomi 'CHIAVE'.
  #                               'COD_RATING_IN', 'COD_RATING_OUT'
  #   Returns:
  #'     @df.diagn  (dataframe) : dataframe in cui é riportato il diagnostico.                             
  
  ## conteggio delle osservazioni per la chiave definita e per cod_rating_in
  df_errors <- df_matrix %>% 
    group_by(CHIAVE,COD_RATING_IN) %>% 
    count()
  
  ## rinomino la colonna 'n' con la stringa 'COUNT_RATING_OUT'
  names(df_errors)[which(names(df_errors)=='n')] <- 'COUNT_RATING_OUT'
  
  ## conteggio delle osservazioni per la chiave definita e per cod_rating_out
  df_errors <- df_errors %>% 
    group_by(CHIAVE,COUNT_RATING_OUT) %>% 
    count()
  
  ## rinomino la colonna 'n' con la stringa 'COUNT_RATING_IN'
  names(df_errors)[which(names(df_errors)=='n')] <- 'COUNT_RATING_IN'
  
  # Individuazione e salvataggio dei valori diversi da 1 in df_errors
  df_errors <- df_errors[which(df_errors$COUNT_RATING_IN != df_errors$COUNT_RATING_OUT),]
  
  # Creazione della concatenazione delle chiavi al fine di creare {error} il diagnostico
  df_diagn <- df_errors
  colnames(df_diagn) <- c('output1','output2','output3')
  return(as.data.frame(df_diagn))
}


valore_campo <-  function(df_table,col_check, val_min, val_max)
{
  
  # Eseguo un controllo del campo "VALORE" (col_check = 'VAL_TD_SS').
  #   Args:
  #'     @df_table  (dataframe)  : dataframe da controllare (ci deve essere il campo CHIAVE 
  #'                               con la chiave da vedere in output)
  #'     @col_check (character)  : colonna da controllare.In questo controllo abbiamo col_check=VAL_TD_SS.
  #'                               Deve contenere val min e val max.
  #'     @val_min   (double)     : valore minimo, se val_min=NULL non considera valore minimo.
  #'     @val_max   (double)     : valore massimo, se val_max=NULL non considera valore massimo. 
  # i val_min e val_max si intendono sempre compresi
  #   Returns: 
  #'     @df_diagn  (dataframe)  : dataframe in cui é riportato il diagnostico.
  
  # Rinomino "col_check" ovvero la colonna "VAL_TD_SS" con la stringa valore 
  names(df_table)[which(names(df_table)==col_check)] <- 'VALORE'
  
  # Check: in df_errors vengono salvate la chiave ed il valore dei "col_check" in cui VALORE è 
  # maggiore del valore massimo o in cui VALORE è minore del valore minimo.
  if(is.null(val_min) & !is.null(val_max)){
    df_errors <- df_table %>% 
      filter(VALORE > val_max) %>% 
      select (CHIAVE,VALORE)
  }
  else if(!is.null(val_min) & is.null(val_max)){
    df_errors <- df_table %>% 
      filter(VALORE < val_min) %>% 
      select (CHIAVE,VALORE)
  }
  else if(!is.null(val_min) & !is.null(val_max)){
    df_errors <- df_table %>% 
      filter(VALORE > val_max | VALORE < val_min) %>%
      select (CHIAVE,VALORE)
  }
  else{df_errors <- data.frame(matrix(nrow = 0, ncol=2)) }
  
  
  # Creazione della concatenazione delle chiavi al fine di creare {error} il df diagnostico costituito
  # da due colonne (output 1 ed output 2)
  df_diagn <- df_errors
  colnames(df_diagn) <- c('output1','output2')
  return(as.data.frame(df_diagn))
}

valore_campo_string <-  function(df_table,col_check, val_string)
{
  # Eseguo un controllo sul campo stringa "VALORE" 
  #  Args:
  #'    @df_table   (dataframe)          : dataframe da controllare (ci deve essere il campo CHIAVE 
  #'                                       con la chiave da vedere in output)
  #'    @col_check  (character)          : colonna da controllare.
  #'                                       In questo controllo abbiamo col_check='COD_LT_ST'.
  #'    @val_string (vettore di stringhe): vettore contenete la lista delle stringhe che il campo "COL_CHECK" può assumere. 
  #'                                    
  #  Returns:
  #'    @df_diagn   (dataframe)          : dataframe in cui è riportato il diagnostico
  
  #rinomino "col_check" ovvero la colonna COD_CORRETTIVI con la stringa VALORE
  names(df_table)[names(df_table)==col_check] <- 'VALORE'
  
  # Check: vengono salvate in df_errors la chiave ed il valore dei cod_macroaggregato il cui
  # VALORE non è in val_string.
  df_errors <- df_table %>% filter( !(VALORE %in% val_string)) %>% select (CHIAVE,VALORE)
  
  # Creazione della concatenazione delle chiavi al fine di creare {error} il diagnostico 
  df_diagn <- df_errors
  colnames(df_diagn) <- c('output1','output2')
  return(as.data.frame(df_diagn))
}

valore_campo_string_startwith <-  function(df_table,col_check, val_string)
{
  # Eseguo un controllo sul campo stringa "VALORE" 
  #  Args:
  #'    @df_table   (dataframe)          : dataframe da controllare (ci deve essere il campo CHIAVE 
  #'                                       con la chiave da vedere in output)
  #'    @col_check  (character)          : colonna da controllare.
  #'    @val_string (vettore di stringhe): vettore contenente la lista delle stringhe con cui il campo "COL_CHECK" 
  #'                                       può cominciare. 
  #'                                    
  #  Returns:
  #'    @df_diagn   (dataframe)          : dataframe in cui è riportato il diagnostico
  
  #rinomino "col_check" con la stringa VALORE
  names(df_table)[names(df_table)==col_check] <- 'VALORE'
  
  # Check: vengono salvate in df_errors la chiave ed il valore di col_check  il cui
  # VALORE non comincia con val_string.
  df_errors <- df_table %>% filter( !(startsWith(VALORE, val_string))) %>% select (CHIAVE,VALORE)
  
  
  # Creazione della concatenazione delle chiavi al fine di creare {error} il diagnostico 
  df_diagn <- df_errors
  colnames(df_diagn) <- c('output1','output2')
  return(as.data.frame(df_diagn))
}


date_check_da_a <- function(df_table,exemp_correttive, exemp_step)
{
  #Eeguo controllo su DAT_PERIODO
  #  Args:
  #'    @df_table         (dataframe) : dataframe da controllare (ci deve essere il campo CHIAVE 
  #'                                    con la chiave da vedere in output)
  #'    @exemp_correttive (character) : specifica correttiva
  #'    @exemp_step       (character) : specifico step
  #  Return:
  #'    @df_diagn         (dataframe) : dataframe in cui è riportato il diagnostico
  
  # togliamo le eccezioni definite in input
  df_table <- df_table %>% filter(!(COD_CORRETTIVI %in% exemp_correttive & COD_STEP %in% exemp_step))
  
  # Check: Vengono salvate in df_errors le chiavi dei cod_macroaggregato in cui DAT_PERIODO_DA>DAT_PERIODO_A
  df_errors <- df_table %>% filter(DAT_PERIODO_DA>DAT_PERIODO_A) %>% select(CHIAVE)
  
  # Creazione della concatenazione delle chiavi al fine di creare {error} il df diagnostico costituito 
  # dalla colonna output1
  df_diagn <- df_errors
  colnames(df_diagn) <- c('output1')
  return(as.data.frame(df_diagn))
}

date_check_sovrapposizione <- function(df_table)
{
  # Eseguo controllo sul numero di righe di corr_temp 
  #  Args:
  #'   @df_table (dataframe): dataframe da controllare (ci deve essere il campo CHIAVE 
  #'                          con la chiave da vedere in output)
  #  Return:
  #'   @df_diagn (dataframe): dataframe in cui è riportato il diagnostico
  
  
  #viene salvata in elenco_correttivi la colonna chiave restituita senza ripetizioni 
  #da df_table come vettore
  elenco_correttivi <- unique('df_table$CHIAVE')
  
  #in corr_temp viene salvata l'esecuzione del filtro 
  for(i in 1:length(elenco_correttivi))
  {
    corr_temp <- df_table %>% filter(CHIAVE==elenco_correttivi[i]) %>% arrange(DAT_PERIODO_DA)
    
    # se il numero di righe di corr_temp è maggiore di 1, l'esito sarà TRUE, 
    #  e verrà scritto nel diagnostico 
    if(nrow(corr_temp)>1)
    {
      corr_temp$esito <- c(TRUE,corr_temp$DAT_PERIODO_A[1:(nrow(corr_temp)-1)] < corr_temp$DAT_PERIODO_DA[2:(nrow(corr_temp))])
    } else {corr_temp$esito <- TRUE}
    
    # se il numero di righe di corr_temp è uguale ad 1, l'esito sarà FALSE
    if(i==1) 
    {
      df_errors <- corr_temp %>% filter(esito==FALSE) %>% select(CHIAVE,DAT_PERIODO_DA)
    } else
    {
      df_errors <- rbind(df_errors,corr_temp %>% filter(esito==FALSE) %>% select(CHIAVE,DAT_PERIODO_DA))
    }
    
  }
  
  # Creazione della concatenazione delle chiavi al fine di creare {error} il df diagnostico 
  # costituito da due colonne (output1 ed output2) 
  df_diagn <- df_errors
  colnames(df_diagn) <- c('output1','output2')
  return(as.data.frame(df_diagn))
  
  
  
}

step_2_check_step_0_1 <- function(df_table)
{
  #   Args:
  #'    @df_table (dataframe) : dataframe da controllare (ci deve essere il campo CHIAVE 
  #'                            con la chiave da vedere in output)
  #   Return: 
  #'    @df_diagn (dataframe) : dataframe in cui è riportato il diagnostico
  
  
  #il risultato del filtro eseguito su COD_CORRETTIVI e COD_STEP viene salvato in df_table
  df_table <- df_table %>% filter(COD_CORRETTIVI %in% c('MORATORIA') & COD_STEP %in% c('0','1','2'))
  
  #il risultato del filtro eseguito su COD_STEP viene salvato in df_table_step_2
  df_table_step_2 <- df_table %>% filter(COD_STEP %in% c('2'))
  
  #il risultato del filtro eseguito su COD_STEP viene salvato in  df_table_step_0_1
  df_table_step_0_1 <- df_table %>% filter(COD_STEP %in% c('1','0'))
  
  #il risultato del filtro eseguito su df_table_step_2 viene salvato in df_errors
  df_errors <- df_table_step_2 %>% filter(!(CHIAVE %in% unique(df_table_step_0_1$CHIAVE))) %>% select (CHIAVE)
  
  # Creazione della concatenazione delle chiavi al fine di creare {error} il diagnostico
  # costituito da una colonna (output1)
  df_diagn <- df_errors
  colnames(df_diagn) <- c('output1')
  return(as.data.frame(df_diagn))
  
}

step_2_check <- function(df_table)
{
  # Eseguo controllo su casistiche in cui COD_CORRETTIVI='MORATORIA' E COD_STEP='2'.
  #   Args: 
  #'    @df_table (dataframe) : dataframe da controllare (ci deve essere il campo CHIAVE 
  #'                            con la chiave da vedere in output)
  #   Return:
  #'    @df_diagn (dataframe) : dataframe in cui è riportato il diagnostico
  
  # Togliere tutto quello che non è moratoria, step2
  
  # vengono salvate in df_table la CHIAVE, la PRC_CORRETTIVA e la PRC_LIV_MIN
  # dei COD_MACROAGGREGATO caratterizzati da COD_CORRETTIVI='MORATORIA' e COD_STEP='2', 
  # dopo aver effettuato un raggruppamento per chiave e senza riportare eventuali
  # duplicati. 
  df_table <- df_table %>% filter(COD_CORRETTIVI =='MORATORIA'& COD_STEP=='2') %>% select(CHIAVE,PRC_CORRETTIVA,PRC_LIV_MIN)  %>% unique() %>%  group_by(CHIAVE)
  
  #Check
  df_errors <- group_data(df_table)
  df_errors$rows_count <- sapply(df_errors$.rows,function (x) length(x))
  
  # Vengono salvate nel df_diagn le chiavi dei cod_macroaggregati che hanno in df_errors
  # il numero di righe maggiore di 1. 
  # il df_diagn è un dataframe costituito da una colonna (output1)
  df_diagn <- df_errors %>% filter(rows_count>1) %>% select(CHIAVE)
  colnames(df_diagn) <- c('output1')
  return(as.data.frame(df_diagn))
  
}

stessa_max_data <- function(df_table)
{
  # Eseguo controllo su max_dat_data
  #   Args:
  #'     @df_table (dataframe) : dataframe da controllare (ci deve essere il campo CHIAVE 
  #'                            con la chiave da vedere in output)
  #   Return: 
  #'     @df_diagn (dataframe) : dataframe in cui è riportato il diagnostico
  
  
  macroaggr_all <- data.frame(CHIAVE=unique(df_table$CHIAVE))
  
  #viene salvato in max_dat_data il valore massimo assunto da DAT_DATA di df_table
  max_dat_data <- max(df_table$DAT_DATA)
  
  #vengono salvate in df_table le chiavi dei cod_macro che hanno DAT_DATA ==max_dat_data
  df_table <- df_table %>% filter(DAT_DATA ==max_dat_data) %>% select(CHIAVE)
  
  #viene salvato in df_errors il risultato del filtro eseguito su macroaggr_all
  df_errors <- macroaggr_all %>% filter(!(CHIAVE %in% df_table$CHIAVE))
  
  # Creazione della concatenazione delle chiavi al fine di creare {error} il diagnostico
  df_diagn <- df_errors 
  colnames(df_diagn) <- c('output1')
  return(as.data.frame(df_diagn))
  
}

check_campi_popolati <- function(df_table,tipo_corr,tipo_step=NA,campi_obbl)
{
  # Eseguo un cotrollo sul campo tipo_step 
  #    Args: 
  #'      @df_table   (dataframe)           : dataframe da controllare (ci deve essere il campo CHIAVE 
  #'                                          con la chiave da vedere in output)
  #'      @tipo_corr  (character)           : tipo correttivi.
  #'      @tipo_step  (vettore di stringhe) : tipo step. Se non specificato di default NA.
  #'      @campi_obbl (vettore di stringhe) : campi obbligatori del df: 'DAT_PERIODO_DA', 'DAT_PERIODO_A',
  #'                                         'PRC_LIV_MIN')
  #    Return:
  #'      @df_diagn   (dataframe)           : dataframe in cui è riportato il diagnostico  
  
  #se tipo_step assume valore nullo vengono salvati in df_table la CHIAVE, la DAT_PERIODO_DA,
  #la DAT_PERIODO_A, la PRC_CORRETTIVA di tutti i cod_macroaggregato caratterizzati da 
  #COD_CORRETTIVI=tipo_corr.In tal caso vengono poi salvate le chiavi dei cod_macroaggregato 
  #del df_table che hanno la somma delle righe nulle maggiore di 0.
  
  #se tipo_step non assume valore nullo vengono salvate in df_table la CHIAVE, la DAT_PERIODO_DA, 
  #la DAT_PERIODO_A, la PRC_CORRETTIVA di tutti i cod_macroaggregato caratterizzati da 
  #COD_CORRETTIVI=tipo_corr e COD_STEP contenuto in tipo_step.In tal caso vengono poi salvate le 
  #chiavi dei cod_macroaggregato che hanno la somma delle righe nulla maggiore di 0.
  
  if(is.na(tipo_step))
  {
    df_table <- df_table %>% filter(COD_CORRETTIVI == tipo_corr) 
    df_table <- df_table[,c('CHIAVE',campi_obbl)]
    
    df_errors <- df_table[rowSums(is.na(df_table)) > 0, ] %>% select(CHIAVE)
  } else {
    df_table <- df_table %>% filter(COD_CORRETTIVI == tipo_corr & COD_STEP %in% tipo_step) 
    df_table <- df_table[,c('CHIAVE',campi_obbl)]
    
    df_errors <- df_table[rowSums(is.na(df_table)) > 0, ] %>% select(CHIAVE)
    
  }
  
  # Creazione della concatenazione delle chiavi al fine di creare {error} il diagnostico
  df_diagn <- df_errors 
  colnames(df_diagn) <- c('output1')
  return(as.data.frame(df_diagn))
}

data_max_tra_tabelle <- function(df_table1,col1,df_table2,col2)
{
  #   Args:
  #'      @df_table1 (dataframe) : dataframe 
  #'      @col1      ()          :
  #'      @df_table2 (dataframe) : dataframe in cui deve esservi riportata la chiave
  #'      @col2      ()          :
  #   Return:
  #'      @df_diagn  (dataframe) : dataframe in cui è riportato il diagnostico  
  
  
  max_date_1 <- as.Date(sapply(df_table1 %>% select(all_of(col1)), function(x) max(x)))
  max_date_2 <- as.Date(sapply(df_table2 %>% select(all_of(col2)), function(x) max(x)))
  
  if(max_date_1<max_date_2)
  {
    names(df_table2)[which(names(df_table2)==col2)] <- 'DATE'
    df_errors <- df_table2 %>% filter (DATE > max_date_1) %>% select(CHIAVE)
  } else df_errors <- data.frame(CHIAVE=NA)
  
  # Creazione della concatenazione delle chiavi al fine di creare {error} il diagnostico 
  df_diagn <- df_errors 
  colnames(df_diagn) <- c('output1')
  return(as.data.frame(df_diagn))
  
}

campo_tra_tabelle <- function(df_table1,col1,df_table2,col2)
{
  #   Args:
  #'    @df_table1 (dataframe) :
  #'    @col1      ( )         :
  #'    @df_table2 (dataframe) :
  #'    @col2      ( )         :
  #  Return:
  #'    @df_diagn  (dataframe) : dataframe in cui è riportato il diagnostico
  campo1 <- unique(df_table1 %>% select(all_of(col1))) %>% unlist(use.names = FALSE)
  campo2 <- unique(df_table2 %>% select(all_of(col2))) %>% unlist(use.names = FALSE)
  
  df_errors <- data.frame(CHIAVE=campo2[!(campo2 %in% campo1)]) 
  
  # Creazione della concatenazione delle chiavi al fine di creare {error} il diagnostico
  df_diagn <- df_errors 
  colnames(df_diagn) <- c('output1')
  return(as.data.frame(df_diagn))
  
}

controllo_num_char <- function (df_table, col_check, num_char)
{
  # Eseguo un controllo sul campo stringa "VALORE" 
  #  Args:
  #'    @df_table   (dataframe)          : dataframe da controllare (ci deve essere il campo CHIAVE 
  #'                                       con la chiave da vedere in output)
  #'    @col_check  (character)          : colonna da controllare.
  #'    @num_char   (integer)            : numero di caratteri che la stringa col_check deve avere. 
  #'                                    
  #  Returns:
  #'    @df_diagn   (dataframe)          : dataframe in cui è riportato il diagnostico
  
  #rinomino "col_check" con la stringa VALORE
  names(df_table)[names(df_table)==col_check] <- 'VALORE'
  
  # Check: vengono salvate in df_errors la chiave ed il valore dei cod_macroaggregato il cui
  # VALORE non è in val_string.
  df_errors <- df_table %>% filter(nchar(VALORE) != num_char) %>% select (CHIAVE,VALORE)
  
  # Creazione della concatenazione delle chiavi al fine di creare {error} il diagnostico 
  df_diagn <- df_errors
  colnames(df_diagn) <- c('output1','output2')
  return(as.data.frame(df_diagn))
  
}

