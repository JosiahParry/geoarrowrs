#' The position of the single geometry column in a data frame
#'
#' @noRd
geometry_column <- function(
  x,
  arg = rlang::caller_arg(x),
  call = rlang::caller_env()
) {
  if (!is.data.frame(x)) {
    cli::cli_abort("{.arg {arg}} must be a data frame.", call = call)
  }

  hits <- which(vapply(x, is_geometry_column, logical(1)))

  if (length(hits) == 0L) {
    cli::cli_abort("{.arg {arg}} has no GeoArrow geometry column.", call = call)
  }
  if (length(hits) > 1L) {
    cli::cli_abort(
      c(
        "{.arg {arg}} has more than one GeoArrow geometry column.",
        i = "Found {.field {names(x)[hits]}}."
      ),
      call = call
    )
  }

  hits[[1L]]
}

#' @noRd
is_geometry_column <- function(x) {
  inherits(x, "geoarrow_vctr") || inherits(x, "nanoarrow_vctr")
}

#' Disambiguate the names two joined frames share
#'
#' @noRd
suffix_names <- function(x_names, y_names, suffix) {
  shared <- intersect(x_names, y_names)
  x_names[x_names %in% shared] <- paste0(
    x_names[x_names %in% shared],
    suffix[[1L]]
  )
  y_names[y_names %in% shared] <- paste0(
    y_names[y_names %in% shared],
    suffix[[2L]]
  )
  list(x = x_names, y = y_names)
}

#' Join two data frames on a spatial relationship
#'
#' Attaches the columns of `y` to each row of `x` that it relates to, the way
#' [merge()] attaches them on a shared key. The geometry of `x` is kept and the
#' geometry of `y` is dropped.
#'
#' @details
#' A row of `x` matching several rows of `y` is repeated once per match, so the
#' result is usually longer than `x`. With `left = TRUE` a row matching nothing
#' is kept once with `NA` in every column from `y`; with `left = FALSE` it is
#' dropped.
#'
#' `predicate` is any of the sparse predicates, such as [ga_sparse_within()] or
#' [ga_sparse_touches()]. It is called once as `predicate(x_geometry,
#' y_geometry)`, so points in polygons is `ga_sparse_within()` and polygons
#' holding points is `ga_sparse_contains()`.
#'
#' Columns the two frames share are suffixed rather than overwritten. Both
#' frames need exactly one GeoArrow geometry column.
#'
#' @param x,y data frames, each with one GeoArrow geometry column
#' @param predicate a sparse predicate, by default [ga_sparse_intersects()]
#' @param suffix the pair of suffixes added to column names found in both frames
#' @param left whether to keep rows of `x` that match nothing
#' @returns a data frame with the columns of `x` followed by the non geometry
#'   columns of `y`
#' @export
#' @family topology
#' @examplesIf requireNamespace("sf", quietly = TRUE) && requireNamespace("geoarrow", quietly = TRUE)
#' nc <- as.data.frame(read_shapefile(
#'   system.file("shape/nc.shp", package = "sf")
#' ))
#' counties <- nc[c("NAME", "geometry")]
#' sites <- data.frame(
#'   site = c("a", "b"),
#'   geometry = geoarrow::as_geoarrow_vctr(
#'     ga_xy(c(-78.6, -80.8), c(35.8, 35.2))
#'   )
#' )
#'
#' # which county each site falls in
#' ga_join(sites, counties, ga_sparse_within)
#'
#' # every pair of neighbouring counties
#' head(ga_join(counties, counties, ga_sparse_touches)[c("NAME_x", "NAME_y")], 3)
ga_join <- function(
  x,
  y,
  predicate = ga_sparse_intersects,
  ...,
  suffix = c("_x", "_y"),
  left = TRUE
) {
  rlang::check_dots_empty()

  x_geo_col <- geometry_column(x)
  y_geo_col <- geometry_column(y)

  if (!is.function(predicate)) {
    cli::cli_abort(
      "{.arg predicate} must be a function, such as {.fn ga_sparse_intersects}."
    )
  }
  if (!is.character(suffix) || length(suffix) != 2L) {
    cli::cli_abort("{.arg suffix} must be a character vector of length 2.")
  }

  hits <- as.vector(predicate(x[[x_geo_col]], y[[y_geo_col]]))
  if (length(hits) != nrow(x)) {
    cli::cli_abort(
      "{.arg predicate} must return one element per row of {.arg x}, not {length(hits)}."
    )
  }

  # a null element means no bounding box to search with, which matches nothing
  hits <- lapply(hits, function(h) if (is.null(h)) integer() else as.integer(h))
  n <- lengths(hits)

  if (left) {
    n[n == 0L] <- 1L
    y_ids <- unlist(lapply(hits, function(h) {
      if (length(h) == 0L) NA_integer_ else h
    }))
  } else {
    y_ids <- unlist(hits)
  }
  x_ids <- rep.int(seq_len(nrow(x)), n)
  y_ids <- if (is.null(y_ids)) integer() else y_ids

  y_keep <- y[-y_geo_col]
  names <- suffix_names(names(x), names(y_keep), suffix)

  out <- cbind(
    rlang::set_names(x[x_ids, , drop = FALSE], names$x),
    rlang::set_names(y_keep[y_ids, , drop = FALSE], names$y)
  )
  rownames(out) <- NULL
  out
}
