#' The metrics a sparse search can measure with
#'
#' `"euclidean"` is first because it is the only one `geo` defines between
#' geometries of any type. The rest are point to point alone.
#'
#' @noRd
sparse_metrics <- c("euclidean", "haversine", "geodesic", "rhumb")

#' Find which rows of `y` lie within a distance of each row of `x`
#'
#' Returns the row numbers of `y` whose geometry is no further than `distance`
#' from each row of `x`. This is the sparse form of a distance band join, and
#' the same test as PostGIS `ST_DWithin()`.
#'
#' @details
#' Distance is measured between the geometries themselves, so a point counts
#' when it is within `distance` of the nearest edge of a polygon, not of the box
#' around it. This is why it differs from buffering `x` and intersecting: a
#' buffer approximates its curves with segments, so a point just inside the true
#' radius can fall outside the buffer.
#'
#' `metric` decides what `distance` means. `"euclidean"` measures in the units
#' the coordinates are in, which on longitude and latitude is degrees, and is
#' what Sedona, PostGIS and DuckDB's `ST_DWithin()` all measure. It is the
#' default because it is the only metric `geo` defines between geometries of any
#' type. For longitude and latitude the answer you almost certainly want is
#' `"geodesic"` or `"haversine"`, which measure metres, and which `geo` defines
#' between points alone: a non point geometry with either is an error rather
#' than a planar number wearing a spherical name.
#'
#' The bounding box of each row of `x` is grown far enough to reach `distance`
#' before the tree is searched, so nothing within reach is missed and only the
#' rows that could qualify are measured. `distance` is recycled, so one value
#' covers every row or a different radius can apply to each.
#'
#' A row that matches nothing gives a zero length element, not a null. A null or
#' empty geometry in `x`, or a null `distance`, gives a null element, and a null
#' geometry in `y` is never returned.
#'
#' @param x a GeoArrow geometry array
#' @param y a GeoArrow geometry array
#' @param distance the furthest a row of `y` may be, in the units of `metric`;
#'   length 1 or the same length as `x`
#' @param metric one of `"euclidean"`, `"haversine"`, `"geodesic"`, or
#'   `"rhumb"`. Everything but `"euclidean"` measures between points only.
#' @returns a list array of 1 based row numbers into `y`, the same length as `x`
#' @export
#' @family index
#' @examplesIf requireNamespace("sf", quietly = TRUE) && requireNamespace("geoarrow", quietly = TRUE)
#' nc <- as.data.frame(read_shapefile(
#'   system.file("shape/nc.shp", package = "sf")
#' ))
#' sites <- ga_xy(c(-78.6, -80.8), c(35.8, 35.2))
#'
#' # the counties within a quarter degree of each site
#' as.vector(ga_sparse_dwithin(sites, nc$geometry, 0.25))
#'
#' # between points, within 50km of each other on the ellipsoid
#' other <- ga_xy(c(-78.7, -79.9), c(35.9, 35.4))
#' as.vector(ga_sparse_dwithin(sites, other, 50000, metric = "geodesic"))
ga_sparse_dwithin <- function(x, y, distance, metric = "euclidean") {
  metric <- rlang::arg_match(metric, sparse_metrics)
  ga_sparse_dwithin_impl(x, y, distance, metric)
}

#' Find the rows of `y` nearest each row of `x`
#'
#' Returns the `k` rows of `y` closest to each row of `x`, nearest first, each
#' paired with the distance between them. This is the sparse form of a nearest
#' neighbour search, and the shape [ga_knn_join()] needs.
#'
#' @details
#' Distance is measured between the geometries themselves, not between their
#' bounding boxes, so the nearest edge of a polygon counts rather than the corner
#' of the box around it. `y` is indexed in a packed Hilbert R-tree, which narrows
#' the search to the rows that can win before any exact distance is computed, and
#' the answer is the same as comparing every pair.
#'
#' `metric` decides how far apart two rows are, and so which rows win.
#' `"euclidean"` measures in the units the coordinates are in, which on longitude
#' and latitude is degrees, and ranks neighbours differently from a real distance
#' once the rows are far apart or near a pole. It is the default because it is
#' the only metric `geo` defines between geometries of any type. For longitude
#' and latitude points, `"geodesic"` or `"haversine"` is the answer you want.
#'
#' A row matches fewer than `k` rows only when `max_distance` rules the rest out
#' or `y` is shorter than `k`. A null or empty geometry in `x` gives a null
#' element, and one in `y` is never returned.
#'
#' @inheritParams ga_sparse_dwithin
#' @param k how many rows of `y` to return per row of `x`
#' @param max_distance the furthest a match may be, in the units of `metric`, or
#'   `NULL` for no limit
#' @returns a list array of `row` and `distance` pairs, the same length as `x`,
#'   where `row` is a 1 based row number into `y`
#' @export
#' @family index
#' @examplesIf requireNamespace("sf", quietly = TRUE) && requireNamespace("geoarrow", quietly = TRUE)
#' nc <- as.data.frame(read_shapefile(
#'   system.file("shape/nc.shp", package = "sf")
#' ))
#' sites <- ga_xy(c(-78.6, -80.8), c(35.8, 35.2))
#'
#' # the three counties nearest each site, with their distances
#' as.vector(ga_sparse_knn(sites, nc$geometry, k = 3))
ga_sparse_knn <- function(
  x,
  y,
  k = 1,
  max_distance = NULL,
  metric = "euclidean"
) {
  metric <- rlang::arg_match(metric, sparse_metrics)
  ga_sparse_knn_impl(x, y, k, max_distance, metric)
}
