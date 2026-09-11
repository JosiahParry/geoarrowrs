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
#' Both sides are taken in Arrow rather than subset in R. Row subsetting a data
#' frame of a few million rows costs seconds and `Take()` costs milliseconds,
#' and the result is the Arrow table every other function here returns.
#' `x_idx` and `y_idx` are zero based, and a null in `y_idx` gives a row of
#' `NA`, which is what an unmatched row of `x` needs.
#'
#' @noRd
bind_matches <- function(x, y, y_geo_col, x_idx, y_idx, suffix) {
  x_tbl <- arrow::as_arrow_table(x)
  y_tbl <- arrow::as_arrow_table(y[-y_geo_col])
  names <- suffix_names(names(x_tbl), names(y_tbl), suffix)

  out <- x_tbl$Take(x_idx)$RenameColumns(names$x)
  taken <- y_tbl$Take(y_idx)
  for (i in seq_along(names$y)) {
    out[[names$y[[i]]]] <- taken[[i]]
  }
  out
}

#' The zero based index `Take()` wants, from the one based rows we return
#'
#' @noRd
take_index <- function(rows) {
  arrow::call_function(
    "subtract",
    arrow::as_arrow_array(rows)$cast(arrow::int64()),
    arrow::Scalar$create(1L, arrow::int64())
  )
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
#' @returns an Arrow table with the columns of `x` followed by the non geometry
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

  hits <- arrow::as_arrow_array(predicate(x[[x_geo_col]], y[[y_geo_col]]))
  if (hits$length() != nrow(x)) {
    cli::cli_abort(
      "{.arg predicate} must return one element per row of {.arg x}, not {hits$length()}."
    )
  }

  pairs <- arrow::as_arrow_array(ga_sparse_pairs(hits, left = left))
  bind_matches(
    x,
    y,
    y_geo_col,
    take_index(pairs$GetFieldByName("x")),
    take_index(pairs$GetFieldByName("y")),
    suffix
  )
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
#' @returns an Arrow table with the columns of `x`, the non geometry columns of
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

  out <- bind_matches(
    x,
    y,
    y_geo_col,
    take_index(x_ids),
    take_index(y_ids),
    suffix
  )
  if (!is.null(distance)) {
    out[[distance]] <- arrow::as_arrow_array(
      if (is.null(dists)) numeric() else as.numeric(dists)
    )
  }
  out
}

#' Keep the rows of a data frame that relate to another spatially
#'
#' Filters `x` down to the rows whose geometry relates to any row of `y`. The
#' columns of `y` are not attached, which is the difference from [ga_join()].
#'
#' @details
#' A row of `x` is kept when the predicate finds it at least one match in `y`,
#' so `ga_filter(sites, counties, ga_sparse_within)` keeps the sites that fall
#' in any county. This is `ST_Filter()`, and the same answer as
#' `ga_join(x, y, predicate, left = FALSE)` with the columns of `y` dropped and
#' the rows of `x` not repeated.
#'
#' @inheritParams ga_join
#' @returns an Arrow table holding the rows of `x` that matched
#' @export
#' @family topology
#' @examplesIf requireNamespace("sf", quietly = TRUE) && requireNamespace("geoarrow", quietly = TRUE)
#' nc <- as.data.frame(read_shapefile(
#'   system.file("shape/nc.shp", package = "sf")
#' ))
#' sites <- data.frame(
#'   site = c("a", "b", "c"),
#'   geometry = geoarrow::as_geoarrow_vctr(
#'     ga_xy(c(-78.6, -80.8, 0), c(35.8, 35.2, 0))
#'   )
#' )
#'
#' # the third site is in the Atlantic, so it goes
#' ga_filter(sites, nc, ga_sparse_within)
ga_filter <- function(x, y, predicate = ga_sparse_intersects, ...) {
  rlang::check_dots_empty()

  x_geo_col <- geometry_column(x)
  y_geo_col <- geometry_column(y)

  if (!is.function(predicate)) {
    cli::cli_abort(
      "{.arg predicate} must be a function, such as {.fn ga_sparse_intersects}."
    )
  }

  hits <- arrow::as_arrow_array(predicate(x[[x_geo_col]], y[[y_geo_col]]))
  if (hits$length() != nrow(x)) {
    cli::cli_abort(
      "{.arg predicate} must return one element per row of {.arg x}, not {hits$length()}."
    )
  }

  # a null element is a row with no box to search with, which matched nothing
  keep <- arrow::call_function(
    "greater",
    arrow::call_function("list_value_length", hits),
    arrow::Scalar$create(0L)
  )
  arrow::as_arrow_table(x)$Filter(keep)
}
