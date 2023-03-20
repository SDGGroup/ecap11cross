#' out2log_local
#' @description Usa \link[base]{cat} per scrivere messaggi su stdout, così da essere utile in run locali.
#' @export

out2log_local <- function(...) {
  # questa funzione usa cat() per scrivere messaggi su stdout, così da essere utile in run locali (lancio_interfaccia == NO)
  s <- paste(...)
  lapply(strsplit(s, "\n"), function(s) {
    cat(s, "\n")
  })
  invisible(NULL)
}


#' out2log_airflow
#' @description Usa \link[base]{cat} per scrivere messaggi su stderr, così che anche airflow li mostri.
#' @export

out2log_airflow <- function(...) {
  # questa funzione usa cat() per scrivere messaggi su stderr, così che anche airflow li mostri  (lancio_interfaccia == SI)
  s <- paste(...)
  lapply(strsplit(s, "\n"), function(s) {
    cat(s, "\n", file=stderr())
  })
  invisible(NULL)
}