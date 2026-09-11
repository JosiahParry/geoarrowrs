#' How many threads the parallel paths may use, when something caps them
#'
#' `NULL` means no cap, which is every core. `R CMD check` sets
#' `_R_CHECK_LIMIT_CORES_`, and CRAN sets it on the check farm, so that is the
#' signal to come down to two. `OMP_THREAD_LIMIT` is what CRAN sets on some
#' check machines and is respected as a ceiling.
#'
#' @noRd
geoarrowrs_thread_cap <- function() {
  if (nzchar(Sys.getenv("_R_CHECK_LIMIT_CORES_"))) {
    return(2L)
  }

  limit <- suppressWarnings(as.integer(Sys.getenv("OMP_THREAD_LIMIT")))
  if (!is.na(limit) && limit > 0L) {
    return(limit)
  }

  NULL
}

.onLoad <- function(libname, pkgname) {
  if (is.null(getOption("geoarrowrs.thread_pool"))) {
    cap <- geoarrowrs_thread_cap()
    if (!is.null(cap)) {
      options(geoarrowrs.thread_pool = cap)
    }
  }

  # the Rust side holds the cap rather than reading the option when it needs
  # it, because a kernel registered with Acero is called on one of Arrow's
  # worker threads, and the R API may only be touched from R's own
  ga_set_thread_pool(as.integer(getOption("geoarrowrs.thread_pool", 0L)))

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
