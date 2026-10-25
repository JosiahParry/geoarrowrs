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
#' from each row of `x`. `ga_sparse_dwithin()` is the sparse form of a distance
#' band join and runs the same test as PostGIS `ST_DWithin()`.
#'
#' @details
#' `ga_sparse_dwithin()` measures distance between the geometries themselves,
#' so it counts a point that lies within `distance` of the nearest edge of a
#' polygon, not of the box around it. That is why it differs from buffering `x`
#' and intersecting: [ga_buffer()] approximates curves with segments, so a point
#' just inside the true radius can fall outside the buffer.
#'
#' `metric` sets what `distance` means. With `"euclidean"`,
#' `ga_sparse_dwithin()` measures in the units of the coordinates, which on
#' longitude and latitude is degrees, as Sedona, PostGIS and DuckDB's
#' `ST_DWithin()` all do. It is the default because `geo` defines no other
#' metric between geometries of any type. For longitude and latitude you almost
#' certainly want `"geodesic"` or `"haversine"`, which work in metres but which
#' `geo` defines between points alone: `ga_sparse_dwithin()` raises an error for
#' a non point geometry with either, rather than return a planar number wearing
#' a spherical name.
#'
#' Before it searches the tree, `ga_sparse_dwithin()` grows the bounding box of
#' each row of `x` far enough to reach `distance`, so it misses nothing within
#' reach and measures only the rows that could qualify. It recycles `distance`,
#' so you can give one value for every row or a different radius for each.
#'
#' `ga_sparse_dwithin()` returns a zero length element for a row that matches
#' nothing, not a null. It returns a null element for a null or empty geometry
#' in `x` or a null `distance`, and never returns a null geometry from `y`.
#'
#' @param x a GeoArrow geometry array
#' @param y a GeoArrow geometry array
#' @param distance the furthest a row of `y` may be, in the units of `metric`;
#'   length 1 or the same length as `x`
#' @param metric one of `"euclidean"`, `"haversine"`, `"geodesic"`, or
#'   `"rhumb"`. Use `"euclidean"` for geometries other than points.
#' @returns a list array of 1 based row numbers into `y`, the same length as `x`
#' @export
#' @family index
#' @examplesIf requireNamespace("sf", quietly = TRUE) && requireNamespace("geoarrow", quietly = TRUE)
#' nc <- as.data.frame(read_shapefile(
#'   system.file("shape/nc.shp", package = "sf")
#' ))
#' sites <- ga_point(c(-78.6, -80.8), c(35.8, 35.2))
#'
#' # the counties within a quarter degree of each site
#' as.vector(ga_sparse_dwithin(sites, nc$geometry, 0.25))
#'
#' # between points, within 50km of each other on the ellipsoid
#' other <- ga_point(c(-78.7, -79.9), c(35.9, 35.4))
#' as.vector(ga_sparse_dwithin(sites, other, 50000, metric = "geodesic"))
ga_sparse_dwithin <- function(x, y, distance, metric = "euclidean") {
  metric <- rlang::arg_match(metric, sparse_metrics)
  ga_sparse_dwithin_impl(x, y, distance, metric)
}

#' Find the rows of `y` nearest each row of `x`
#'
#' Returns the `k` rows of `y` closest to each row of `x`, nearest first, each
#' paired with the distance between them. `ga_sparse_knn()` is the sparse form
#' of a nearest neighbour search, and returns the shape [ga_knn_join()] needs.
#'
#' @details
#' `ga_sparse_knn()` measures distance between the geometries themselves, not
#' between their bounding boxes, so it counts the nearest edge of a polygon
#' rather than the corner of the box around it. It indexes `y` in a packed
#' Hilbert R-tree to narrow the search to the rows that can win before it
#' computes any exact distance, and returns the same answer as comparing every
#' pair.
#'
#' `metric` sets how `ga_sparse_knn()` measures how far apart two rows are, and
#' so which rows win. With `"euclidean"` it measures in the units of the
#' coordinates, which on longitude and latitude is degrees, and so ranks
#' neighbours differently from a real distance once the rows are far apart or
#' near a pole. It is the default because `geo` defines no other metric between
#' geometries of any type. For longitude and latitude points, you want
#' `"geodesic"` or `"haversine"`.
#'
#' `ga_sparse_knn()` returns fewer than `k` rows for a row of `x` only when
#' `max_distance` rules the rest out or `y` is shorter than `k`. It returns a
#' null element for a null or empty geometry in `x`, and never returns one from
#' `y`.
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
#' sites <- ga_point(c(-78.6, -80.8), c(35.8, 35.2))
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
