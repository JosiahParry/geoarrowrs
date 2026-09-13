# sdf backend.
#
# sdf takes PostGIS's names, which are sf's names too for most of these, so the
# methods that serve both generics are registered on both in sf-compat.R and
# are not repeated here. What is left is what sdf asks for and sf does not: the
# three required generics, and the operations sf answers for without
# dispatching, which sdf makes generic and geoarrowrs can therefore implement
# properly rather than by masking sf.

#' A sparse predicate as the list sdf expects
#'
#' sdf asks a predicate for "a sparse matrix list representation", whose
#' elements are the row positions of `y` that match. That is what the sparse
#' predicates already return, one row per row of `x` and 1 based, so this only
#' takes it out of Arrow: `st_join()` calls `lengths()` and `[[` on the result,
#' which a list array does not answer to.
#'
#' @noRd
sdf_hits <- function(hits) {
  lapply(as.vector(hits), function(hit) {
    if (is.null(hit)) integer() else as.integer(hit)
  })
}

# required ---------------------------------------------------------------

#' @exportS3Method sdf::is_geometry
#' @noRd
is_geometry.geoarrow_vctr <- function(x) {
  TRUE
}

#' @exportS3Method sdf::st_extent
#' @noRd
st_extent.geoarrow_vctr <- function(x) {
  # the box array is a struct of four columns, so the extent is their range
  boxes <- ga_bounding_rect(as_ga(x))$children

  c(
    xmin = min(as.vector(boxes$xmin), na.rm = TRUE),
    ymin = min(as.vector(boxes$ymin), na.rm = TRUE),
    xmax = max(as.vector(boxes$xmax), na.rm = TRUE),
    ymax = max(as.vector(boxes$ymax), na.rm = TRUE)
  )
}

#' @exportS3Method sdf::st_collect
#' @noRd
st_collect.geoarrow_vctr <- function(x) {
  ga_out(ga_collect_agg(as_ga(x)))
}

# where sdf's answer is not sf's ------------------------------------------
# sf takes the Arrow array, which is the point of this package. sdf's contract
# is a plain R vector: a numeric from st_area(), a logical from st_is_valid(),
# and a list of row positions from a predicate, which st_join() calls lengths()
# and [[ on.
#
# Neither of the two is named st_area.geoarrow_vctr. S3 dispatch looks in the
# environment the generic was called from before it looks in the generic's own
# method table, so a function of that name sitting in this namespace decides
# both generics wherever this namespace is visible, which is what load_all()
# does under test. Each is named for the generic it serves and registered on
# that one alone.

#' @noRd
st_area_sdf <- function(x, ...) {
  as.vector(ga_unsigned_area(as_ga(x)))
}

#' @noRd
st_is_valid_sdf <- function(x, ...) {
  as.vector(ga_is_valid(as_ga(x)))
}

#' @noRd
st_intersects_sdf <- function(x, y, ...) {
  sdf_hits(ga_sparse_intersects(as_ga(x), as_ga(y)))
}

# predicates, which are what st_join() and st_filter() run on -----------------

#' @exportS3Method sdf::st_contains
#' @noRd
st_contains.geoarrow_vctr <- function(x, y, ...) {
  sdf_hits(ga_sparse_contains(as_ga(x), as_ga(y)))
}

#' @exportS3Method sdf::st_within
#' @noRd
st_within.geoarrow_vctr <- function(x, y, ...) {
  sdf_hits(ga_sparse_within(as_ga(x), as_ga(y)))
}

#' @exportS3Method sdf::st_crosses
#' @noRd
st_crosses.geoarrow_vctr <- function(x, y, ...) {
  sdf_hits(ga_sparse_crosses(as_ga(x), as_ga(y)))
}

#' @exportS3Method sdf::st_covers
#' @noRd
st_covers.geoarrow_vctr <- function(x, y, ...) {
  sdf_hits(ga_sparse_covers(as_ga(x), as_ga(y)))
}

#' @exportS3Method sdf::st_covered_by
#' @noRd
st_covered_by.geoarrow_vctr <- function(x, y, ...) {
  sdf_hits(ga_sparse_covered_by(as_ga(x), as_ga(y)))
}

#' @exportS3Method sdf::st_equals
#' @noRd
st_equals.geoarrow_vctr <- function(x, y, ...) {
  sdf_hits(ga_sparse_equals_topo(as_ga(x), as_ga(y)))
}

#' @exportS3Method sdf::st_touches
#' @noRd
st_touches.geoarrow_vctr <- function(x, y, ...) {
  sdf_hits(ga_sparse_touches(as_ga(x), as_ga(y)))
}

#' @exportS3Method sdf::st_overlaps
#' @noRd
st_overlaps.geoarrow_vctr <- function(x, y, ...) {
  sdf_hits(ga_sparse_overlaps(as_ga(x), as_ga(y)))
}

#' @exportS3Method sdf::st_dwithin
#' @noRd
st_dwithin.geoarrow_vctr <- function(x, y, distance, ...) {
  sdf_hits(ga_sparse_dwithin(as_ga(x), as_ga(y), distance))
}

#' Disjointness is the complement of intersection
#'
#' A bounding box cannot narrow disjointness, so there is no sparse predicate
#' for it and the complement is the same answer.
#'
#' @noRd
#' @exportS3Method sdf::st_disjoint
st_disjoint.geoarrow_vctr <- function(x, y, ...) {
  x <- as_ga(x)
  y <- as_ga(y)
  all_rows <- seq_len(length(y))

  lapply(as.vector(ga_sparse_intersects(x, y)), function(hit) {
    if (is.null(hit)) all_rows else setdiff(all_rows, as.integer(hit))
  })
}

# measurement ------------------------------------------------------------
# PostGIS returns a float, so these come out of Arrow as plain numerics

#' @exportS3Method sdf::st_length
#' @noRd
st_length.geoarrow_vctr <- function(x, ...) {
  as.vector(ga_length_euclidean(as_ga(x)))
}

#' @exportS3Method sdf::st_distance
#' @noRd
st_distance.geoarrow_vctr <- function(x, y, ...) {
  as.vector(ga_dist_euclidean_pairwise(as_ga(x), as_ga(y)))
}

# accessors and tests ----------------------------------------------------

#' @exportS3Method sdf::st_is_empty
#' @noRd
st_is_empty.geoarrow_vctr <- function(x, ...) {
  as.vector(ga_is_empty(as_ga(x)))
}

#' A geometry is simple when it does not cross itself
#'
#' `ga_self_intersections()` gives the crossing points, so an empty result is
#' the answer.
#'
#' @noRd
#' @exportS3Method sdf::st_is_simple
st_is_simple.geoarrow_vctr <- function(x, ...) {
  as.vector(ga_is_empty(ga_self_intersections(as_ga(x))))
}

#' @exportS3Method sdf::st_envelope
#' @noRd
st_envelope.geoarrow_vctr <- function(x, ...) {
  ga_out(ga_envelope(as_ga(x)))
}

#' @exportS3Method sdf::st_num_points
#' @noRd
st_num_points.geoarrow_vctr <- function(x, ...) {
  as.vector(ga_n_coords(as_ga(x)))
}

#' @exportS3Method sdf::st_x
#' @noRd
st_x.geoarrow_vctr <- function(x, ...) {
  as.vector(ga_x(as_ga(x)))
}

#' @exportS3Method sdf::st_y
#' @noRd
st_y.geoarrow_vctr <- function(x, ...) {
  as.vector(ga_y(as_ga(x)))
}

#' @exportS3Method sdf::st_relate
#' @noRd
st_relate.geoarrow_vctr <- function(x, y, ...) {
  as.vector(ga_relate(as_ga(x), as_ga(y)))
}

# st_perimeter() is not registered on purpose: ga_perimeter_geodesic() measures
# on the sphere and PostGIS ST_Perimeter is planar. It stays available under
# sf's name through mask_sf(), where the geodesic reading is what was asked
# for. Nor are st_geometry_type(), st_boundary(), st_num_geometries(),
# st_make_valid() or st_reverse(), which have no binding here yet.
