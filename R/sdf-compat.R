#' A sparse predicate as the list sdf expects
#'
#' sdf asks a predicate for "a sparse matrix list representation", whose
#' elements are the row positions of `y` that match. That is what the sparse
#' predicates already return, one row per row of `x` and 1 based, so this only
#' takes it out of Arrow: `sdf_join()` calls `lengths()` and `[[` on the result,
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

#' @exportS3Method sdf::bounding_box
#' @noRd
bounding_box.geoarrow_vctr <- function(x) {
  # the box array is a struct of four columns, so the extent is their range
  boxes <- ga_bounding_rect(x)$children

  c(
    xmin = min(as.vector(boxes$xmin), na.rm = TRUE),
    ymin = min(as.vector(boxes$ymin), na.rm = TRUE),
    xmax = max(as.vector(boxes$xmax), na.rm = TRUE),
    ymax = max(as.vector(boxes$ymax), na.rm = TRUE)
  )
}

#' @exportS3Method sdf::combine_geometry
#' @noRd
combine_geometry.geoarrow_vctr <- function(x) {
  ga_out(ga_collect_agg(x))
}

# predicates, which are what sdf_join() and sdf_filter() run on --------------

#' @exportS3Method sdf::sdf_intersects
#' @noRd
sdf_intersects.geoarrow_vctr <- function(x, y, ...) {
  sdf_hits(ga_sparse_intersects(x, y))
}

#' @exportS3Method sdf::sdf_contains
#' @noRd
sdf_contains.geoarrow_vctr <- function(x, y, ...) {
  sdf_hits(ga_sparse_contains(x, y))
}

#' @exportS3Method sdf::sdf_within
#' @noRd
sdf_within.geoarrow_vctr <- function(x, y, ...) {
  sdf_hits(ga_sparse_within(x, y))
}

#' @exportS3Method sdf::sdf_crosses
#' @noRd
sdf_crosses.geoarrow_vctr <- function(x, y, ...) {
  sdf_hits(ga_sparse_crosses(x, y))
}

#' @exportS3Method sdf::sdf_covers
#' @noRd
sdf_covers.geoarrow_vctr <- function(x, y, ...) {
  sdf_hits(ga_sparse_covers(x, y))
}

#' @exportS3Method sdf::sdf_covered_by
#' @noRd
sdf_covered_by.geoarrow_vctr <- function(x, y, ...) {
  sdf_hits(ga_sparse_covered_by(x, y))
}

#' @exportS3Method sdf::sdf_equals
#' @noRd
sdf_equals.geoarrow_vctr <- function(x, y, ...) {
  sdf_hits(ga_sparse_equals_topo(x, y))
}

#' @exportS3Method sdf::sdf_touches
#' @noRd
sdf_touches.geoarrow_vctr <- function(x, y, ...) {
  sdf_hits(ga_sparse_touches(x, y))
}

#' @exportS3Method sdf::sdf_overlaps
#' @noRd
sdf_overlaps.geoarrow_vctr <- function(x, y, ...) {
  sdf_hits(ga_sparse_overlaps(x, y))
}

#' Disjointness is the complement of intersection
#'
#' See [st_disjoint()]: a bounding box cannot narrow disjointness, so there is
#' no sparse predicate for it and the complement is the same answer.
#'
#' @noRd
#' @exportS3Method sdf::sdf_disjoint
sdf_disjoint.geoarrow_vctr <- function(x, y, ...) {
  all_rows <- seq_len(length(y))

  lapply(as.vector(ga_sparse_intersects(x, y)), function(hit) {
    if (is.null(hit)) all_rows else setdiff(all_rows, as.integer(hit))
  })
}

# optional ---------------------------------------------------------------

#' @exportS3Method sdf::union_geometry
#' @noRd
union_geometry.geoarrow_vctr <- function(x) {
  ga_out(ga_unary_union(x))
}

#' @exportS3Method sdf::simplify_geometry
#' @noRd
simplify_geometry.geoarrow_vctr <- function(x, tolerance, ...) {
  ga_out(ga_simplify(x, tolerance))
}

#' @exportS3Method sdf::buffer_geometry
#' @noRd
buffer_geometry.geoarrow_vctr <- function(x, distance, ...) {
  ga_out(ga_buffer(x, distance))
}

#' @exportS3Method sdf::centroid
#' @noRd
centroid.geoarrow_vctr <- function(x) {
  ga_out(ga_centroid(x))
}

#' @exportS3Method sdf::convex_hull
#' @noRd
convex_hull.geoarrow_vctr <- function(x) {
  ga_out(ga_convex_hull(x))
}

#' @exportS3Method sdf::concave_hull
#' @noRd
concave_hull.geoarrow_vctr <- function(x, concavity, ...) {
  ga_out(ga_concave_hull(x, concavity, 0))
}

#' @exportS3Method sdf::sdf_area
#' @noRd
sdf_area.geoarrow_vctr <- function(x, ...) {
  as.vector(ga_unsigned_area(x))
}

#' @exportS3Method sdf::sdf_length
#' @noRd
sdf_length.geoarrow_vctr <- function(x, ...) {
  as.vector(ga_length_euclidean(x))
}
