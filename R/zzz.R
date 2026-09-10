.onLoad <- function(libname, pkgname) {
  if (!isTRUE(getOption("geoarrowrs.register_udfs", TRUE))) {
    return(invisible(NULL))
  }

  have <- vapply(
    c("arrow", "dplyr", "geoarrow", "nanoarrow"),
    requireNamespace,
    logical(1),
    quietly = TRUE
  )
  if (!all(have)) {
    return(invisible(NULL))
  }
  if (!isTRUE(arrow::arrow_info()$capabilities[["acero"]])) {
    return(invisible(NULL))
  }

  rlang::try_fetch(register_geoarrow_udfs(), error = function(cnd) NULL)
  invisible(NULL)
}
