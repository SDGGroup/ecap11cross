#' carica_parametri_input
#' @description Inizializza i parametri di input
#' @param .lancio_interfaccia `chr` (SI/NO)
#' @param .id_elaborazione `int`
#' @param .out_version `int`
#' @param .dat_report `date (%Y-%m-%d)`
#' @param .codice_utente `chr`
#' @return `list`
#' @export

carica_parametri_input <- function(.lancio_interfaccia, .id_elaborazione, .out_version, .dat_report, .codice_utente){
  
  if(.lancio_interfaccia == 'SI'){ 
    args = commandArgs(trailingOnly = TRUE)
    if (length(args) < 4) {
      stop("Quattro argomenti attesi:id_elaborazione|out_version|dat_rep|codice_utente", call. = FALSE)
    } else{
      id_elaborazione <- as.integer(args[1])
      out_version <- as.integer(args[2])
      dat_report <- as.Date(args[3])
      codice_utente <- args[4]
    }
  } else {
    # Utilizzata solo per gli sviluppi
    # caricamento manuale per sviluppo
    
    id_elaborazione <- .id_elaborazione
    dat_report <- .dat_report
    out_version <- .out_version
    codice_utente <- .codice_utente
  }
  
  return(list(id_elaborazione = .id_elaborazione, dat_report = .dat_report, out_version = .out_version, codice_utente = .codice_utente))
}
