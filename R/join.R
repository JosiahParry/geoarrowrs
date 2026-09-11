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

#' @noRd
check_suffix <- function(suffix, call = rlang::caller_env()) {
  if (!is.character(suffix) || length(suffix) != 2L) {
    cli::cli_abort(
      "{.arg suffix} must be a character vector of length 2.",
      call = call
    )
  }
}

#' Bind the matched rows of two frames, suffixing the names they share
#'
#' @noRd
bind_matches <- function(x, y, y_geo_col, x_ids, y_ids, suffix) {
  y_keep <- y[-y_geo_col]
  names <- suffix_names(names(x), names(y_keep), suffix)

  out <- cbind(
    rlang::set_names(x[x_ids, , drop = FALSE], names$x),
    rlang::set_names(y_keep[y_ids, , drop = FALSE], names$y)
  )
  rownames(out) <- NULL
  out
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
  check_suffix(suffix)

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

  bind_matches(x, y, y_geo_col, x_ids, y_ids, suffix)
}

#' Join two data frames on nearest neighbours
#'
#' Attaches the columns of the `k` rows of `y` nearest each row of `x`, the way
#' [ga_join()] attaches the ones that relate to it. The distance between each
#' pair comes along as a column.
#'
#' @details
#' Each row of `x` is repeated once per neighbour and the neighbours are ordered
#' nearest first, so the result is `nrow(x) * k` rows unless `max_distance`
#' rules some out. A row left with no neighbour is kept once with `NA` when
#' `left = TRUE` and dropped otherwise.
#'
#' Distance is Euclidean and measured between the geometries themselves, so a
#' point joins to the polygon whose edge is nearest rather than to the one whose
#' bounding box is. Both frames need exactly one GeoArrow geometry column.
#'
#' @inheritParams ga_join
#' @param k how many neighbours to attach to each row of `x`
#' @param max_distance the furthest a neighbour may be, or `NULL` for no limit
#' @param distance the name of the distance column, or `NULL` to leave it out
#' @returns a data frame with the columns of `x`, the non geometry columns of
#'   `y`, and the distance between each pair
#' @export
#' @family index
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
#' # the two counties nearest each site
#' ga_knn_join(sites, counties, k = 2)[c("site", "NAME", "distance")]
ga_knn_join <- function(
  x,
  y,
  k = 1,
  ...,
  max_distance = NULL,
  distance = "distance",
  suffix = c("_x", "_y"),
  left = TRUE
) {
  rlang::check_dots_empty()

  x_geo_col <- geometry_column(x)
  y_geo_col <- geometry_column(y)

  check_suffix(suffix)
  if (!is.null(distance) && !rlang::is_string(distance)) {
    cli::cli_abort("{.arg distance} must be a single column name or `NULL`.")
  }

  hits <- as.vector(ga_sparse_knn(
    x[[x_geo_col]],
    y[[y_geo_col]],
    k = k,
    max_distance = max_distance
  ))

  # a null element means no bounding box to search with, which matches nothing
  n <- vapply(hits, function(h) if (is.null(h)) 0L else nrow(h), integer(1))
  pull <- function(field, empty) {
    unlist(lapply(hits, function(h) {
      if (is.null(h) || nrow(h) == 0L) empty else h[[field]]
    }))
  }

  if (left) {
    y_ids <- pull("row", NA_integer_)
    dists <- pull("distance", NA_real_)
    n[n == 0L] <- 1L
  } else {
    y_ids <- pull("row", NULL)
    dists <- pull("distance", NULL)
  }

  x_ids <- rep.int(seq_len(nrow(x)), n)
  y_ids <- if (is.null(y_ids)) integer() else as.integer(y_ids)

  out <- bind_matches(x, y, y_geo_col, x_ids, y_ids, suffix)
  if (!is.null(distance)) {
    out[[distance]] <- if (is.null(dists)) numeric() else as.numeric(dists)
  }
  out
}
