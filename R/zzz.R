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

#' The methods that serve sf's generic and sdf's alike
#'
#' sdf takes PostGIS's names, which are sf's names too for these, so one method
#' answers both. roxygen allows a block only one `@exportS3Method`, so sf's is
#' the tag and sdf's is registered here when sdf turns up.
#'
#' @noRd
sdf_shared <- c(
  "st_buffer",
  "st_centroid",
  "st_concave_hull",
  "st_convex_hull",
  "st_difference",
  "st_intersection",
  "st_point_on_surface",
  "st_simplify",
  "st_sym_difference",
  "st_union"
)

#' The operations whose answer differs between the two generics
#'
#' sf takes the Arrow array. sdf's contract is a plain R vector. Neither
#' implementation may be named `st_area.geoarrow_vctr`, because S3 dispatch
#' reads the environment the generic was called from before the generic's own
#' method table, so one of that name in this namespace would answer both
#' generics wherever this namespace is visible. Named apart, they can only be
#' reached by registration, and `S3method()` in NAMESPACE cannot point at a
#' function of another name, so both go on here.
#'
#' @noRd
sf_adapted <- c(
  st_area = "st_area_sf",
  st_is_valid = "st_is_valid_sf",
  st_intersects = "st_intersects_sf"
)

#' @noRd
sdf_adapted <- c(
  st_area = "st_area_sdf",
  st_is_valid = "st_is_valid_sdf",
  st_intersects = "st_intersects_sdf"
)

register_adapted <- function(pkg, map) {
  ns <- asNamespace("geoarrowrs")

  for (generic in names(map)) {
    registerS3method(
      generic,
      "geoarrow_vctr",
      get(map[[generic]], envir = ns),
      envir = asNamespace(pkg)
    )
  }
}

#' Register on a package that may not be loaded yet
#'
#' @noRd
when_loaded <- function(pkg, fn) {
  force(pkg)
  force(fn)

  if (isNamespaceLoaded(pkg)) {
    fn()
  }

  setHook(packageEvent(pkg, "onLoad"), function(...) fn())
}

register_sdf_shared <- function(...) {
  ns <- asNamespace("geoarrowrs")

  for (generic in sdf_shared) {
    registerS3method(
      generic,
      "geoarrow_vctr",
      get(paste0(generic, ".geoarrow_vctr"), envir = ns),
      envir = asNamespace("sdf")
    )
  }
}

.onLoad <- function(libname, pkgname) {
  # sf and sdf are both suggested, so wait for them rather than pulling them in
  when_loaded("sf", function() register_adapted("sf", sf_adapted))

  when_loaded("sdf", function() {
    register_sdf_shared()
    register_adapted("sdf", sdf_adapted)
  })

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
