#' Take an argument as a GeoArrow vector
#'
#' Everything here takes and returns `geoarrow_vctr`. A raw array, which is what
#' the `ga_` functions themselves hand back, is wrapped. An `sfc` is refused
#' rather than converted: the point of these is to run on Arrow.
#'
#' @noRd
as_ga <- function(x, arg = rlang::caller_arg(x), call = rlang::caller_env()) {
  if (inherits(x, "geoarrow_vctr")) {
    return(x)
  }
  if (inherits(x, "nanoarrow_array")) {
    return(ga_out(x))
  }

  cli::cli_abort(
    "{.arg {arg}} must be a {.cls geoarrow_vctr}, not {.obj_type_friendly {x}}.",
    call = call
  )
}

#' Wrap a result as a GeoArrow vector
#'
#' geoarrow 0.4.3 does not know the `geoarrow.geometry` or
#' `geoarrow.geometrycollection` extension names, so an aggregate that returns
#' one cannot be wrapped directly. Recoding it as `geoarrow.wkb`, which geoarrow
#' does know, keeps a single representation everywhere rather than leaking a
#' bare array out of one function and not the rest.
#'
#' @noRd
ga_out <- function(x) {
  force(x)
  rlang::try_fetch(
    geoarrow::as_geoarrow_vctr(x),
    error = function(cnd) {
      geoarrow::as_geoarrow_vctr(ga_cast_geometry(x, "wkb"))
    }
  )
}

#' The shared body of every binary predicate
#'
#' `sparse = TRUE` is the list of matching rows the sparse predicates return,
#' `sparse = FALSE` the dense logical matrix sf's argument of that name asks
#' for.
#'
#' @noRd
predicate_out <- function(hits, nx, ny, sparse) {
  if (isTRUE(sparse)) {
    return(hits)
  }

  rows <- as.vector(hits)
  out <- matrix(FALSE, nrow = nx, ncol = ny)
  for (i in seq_len(nx)) {
    if (!is.null(rows[[i]])) {
      out[i, as.integer(rows[[i]])] <- TRUE
    }
  }
  out
}

#' @noRd
sparse_predicate <- function(x, y, sparse, fn) {
  x <- as_ga(x)
  y <- as_ga(y)
  predicate_out(fn(x, y), length(x), length(y), sparse)
}

# sf generics, dispatched on the GeoArrow representation -----------------------

#' @exportS3Method sf::st_area
#' @noRd
st_area.geoarrow_vctr <- function(x, ...) {
  ga_unsigned_area(as_ga(x))
}

#' @exportS3Method sf::st_centroid
#' @noRd
st_centroid.geoarrow_vctr <- function(x, ...) {
  ga_out(ga_centroid(as_ga(x)))
}

#' @exportS3Method sf::st_convex_hull
#' @noRd
st_convex_hull.geoarrow_vctr <- function(x) {
  ga_out(ga_convex_hull(as_ga(x)))
}

#' @exportS3Method sf::st_point_on_surface
#' @noRd
st_point_on_surface.geoarrow_vctr <- function(x) {
  ga_out(ga_interior_point(as_ga(x)))
}

#' @exportS3Method sf::st_minimum_rotated_rectangle
#' @noRd
st_minimum_rotated_rectangle.geoarrow_vctr <- function(x, ...) {
  ga_out(ga_minimum_rotated_rect(as_ga(x)))
}

#' @exportS3Method sf::st_is_valid
#' @noRd
st_is_valid.geoarrow_vctr <- function(x, ...) {
  ga_is_valid(as_ga(x))
}

#' @exportS3Method sf::st_buffer
#' @noRd
st_buffer.geoarrow_vctr <- function(
  x,
  dist,
  nQuadSegs = 30,
  endCapStyle = "ROUND",
  joinStyle = "ROUND",
  mitreLimit = 1,
  singleSide = FALSE,
  ...
) {
  ga_out(ga_buffer(
    as_ga(x),
    dist,
    line_cap = tolower(endCapStyle),
    line_join = tolower(joinStyle),
    miter_limit = mitreLimit
  ))
}

#' @exportS3Method sf::st_simplify
#' @noRd
st_simplify.geoarrow_vctr <- function(x, preserveTopology, dTolerance = 0) {
  ga_out(ga_simplify(as_ga(x), dTolerance))
}

#' @exportS3Method sf::st_segmentize
#' @noRd
st_segmentize.geoarrow_vctr <- function(x, dfMaxLength, ...) {
  ga_out(ga_line_segmentize(as_ga(x), dfMaxLength))
}

#' @exportS3Method sf::st_cast
#' @noRd
st_cast.geoarrow_vctr <- function(x, to, ...) {
  ga_out(ga_cast_geometry(as_ga(x), to))
}

#' @exportS3Method sf::st_triangulate
#' @noRd
st_triangulate.geoarrow_vctr <- function(
  x,
  dTolerance = 0,
  bOnlyEdges = FALSE
) {
  ga_out(ga_triangulate_delaunay(as_ga(x)))
}

#' @exportS3Method sf::st_voronoi
#' @noRd
st_voronoi.geoarrow_vctr <- function(
  x,
  envelope,
  dTolerance = 0,
  bOnlyEdges = FALSE
) {
  if (isTRUE(bOnlyEdges)) {
    return(ga_out(ga_voronoi_edges(as_ga(x))))
  }
  ga_out(ga_voronoi_cells(as_ga(x)))
}

#' @exportS3Method sf::st_union
#' @noRd
st_union.geoarrow_vctr <- function(x, y, ...) {
  if (missing(y)) {
    return(ga_out(ga_unary_union(as_ga(x))))
  }
  ga_out(ga_boolean_union(as_ga(x), as_ga(y)))
}

#' @exportS3Method sf::st_intersection
#' @noRd
st_intersection.geoarrow_vctr <- function(x, y, ...) {
  ga_out(ga_boolean_intersection(as_ga(x), as_ga(y)))
}

#' @exportS3Method sf::st_difference
#' @noRd
st_difference.geoarrow_vctr <- function(x, y, ...) {
  ga_out(ga_boolean_difference(as_ga(x), as_ga(y)))
}

#' @exportS3Method sf::st_sym_difference
#' @noRd
st_sym_difference.geoarrow_vctr <- function(x, y, ...) {
  ga_out(ga_boolean_xor(as_ga(x), as_ga(y)))
}

#' @exportS3Method sf::st_intersects
#' @noRd
st_intersects.geoarrow_vctr <- function(x, y, sparse = TRUE, ...) {
  if (missing(y)) {
    y <- x
  }
  sparse_predicate(x, y, sparse, ga_sparse_intersects)
}

#' sf's `ratio` is GEOS's, which is not geo's `concavity`
#'
#' Both tighten the hull as they fall, and neither is a conversion of the other,
#' so `ratio` is passed straight through as `concavity` and named as sf names it
#' rather than pretending the two scales agree.
#'
#' @noRd
#' @exportS3Method sf::st_concave_hull
st_concave_hull.geoarrow_vctr <- function(
  x,
  ratio,
  ...,
  allow_holes = FALSE,
  length_threshold = 0
) {
  ga_out(ga_concave_hull(as_ga(x), ratio, length_threshold))
}

#' @exportS3Method sf::st_coordinates
#' @noRd
st_coordinates.geoarrow_vctr <- function(x, ...) {
  ga_out(ga_coords(as_ga(x)))
}

#' @exportS3Method sf::st_exterior_ring
#' @noRd
st_exterior_ring.geoarrow_vctr <- function(x, ...) {
  ga_out(ga_exterior_coords(as_ga(x)))
}

#' @exportS3Method sf::st_triangulate_constrained
#' @noRd
st_triangulate_constrained.geoarrow_vctr <- function(x) {
  ga_out(ga_triangulate_delaunay(as_ga(x), constrained = TRUE))
}

# sf non-generics, installed by mask_sf() --------------------------------------

#' @noRd
st_distance <- function(x, y, ..., by_element = FALSE, metric = "euclidean") {
  x <- as_ga(x)
  y <- if (missing(y)) x else as_ga(y)

  if (isTRUE(by_element)) {
    return(pairwise_distance(x, y, metric))
  }

  ga_cross_distance(x, y, metric)
}

#' @noRd
pairwise_distance <- function(x, y, metric) {
  switch(
    rlang::arg_match(metric, c("euclidean", "haversine", "geodesic", "rhumb")),
    euclidean = ga_dist_euclidean_pairwise(x, y),
    haversine = ga_dist_haversine_pairwise(x, y),
    geodesic = ga_dist_geodesic_pairwise(x, y),
    rhumb = ga_dist_rhumb_pairwise(x, y)
  )
}

#' @noRd
st_length <- function(x, ...) {
  ga_length_euclidean(as_ga(x))
}

#' @noRd
st_perimeter <- function(x, ...) {
  ga_perimeter_geodesic(as_ga(x))
}

#' @noRd
st_dimension <- function(x, ...) {
  ga_dimension(as_ga(x))
}

#' @noRd
st_is_empty <- function(x) {
  ga_is_empty(as_ga(x))
}

#' @noRd
st_contains <- function(x, y, sparse = TRUE, ...) {
  sparse_predicate(x, y, sparse, ga_sparse_contains)
}

#' @noRd
st_contains_properly <- function(x, y, sparse = TRUE, ...) {
  sparse_predicate(x, y, sparse, ga_sparse_contains_properly)
}

#' @noRd
st_covers <- function(x, y, sparse = TRUE, ...) {
  sparse_predicate(x, y, sparse, ga_sparse_covers)
}

#' @noRd
st_covered_by <- function(x, y, sparse = TRUE, ...) {
  sparse_predicate(x, y, sparse, ga_sparse_covered_by)
}

#' @noRd
st_crosses <- function(x, y, sparse = TRUE, ...) {
  sparse_predicate(x, y, sparse, ga_sparse_crosses)
}

#' @noRd
st_equals <- function(x, y, sparse = TRUE, ...) {
  sparse_predicate(x, y, sparse, ga_sparse_equals_topo)
}

#' @noRd
st_overlaps <- function(x, y, sparse = TRUE, ...) {
  sparse_predicate(x, y, sparse, ga_sparse_overlaps)
}

#' @noRd
st_touches <- function(x, y, sparse = TRUE, ...) {
  sparse_predicate(x, y, sparse, ga_sparse_touches)
}

#' @noRd
st_within <- function(x, y, sparse = TRUE, ...) {
  sparse_predicate(x, y, sparse, ga_sparse_within)
}

#' Disjointness as the complement of intersection
#'
#' There is no sparse disjoint predicate here on purpose: a bounding box cannot
#' narrow disjointness, so the answer is nearly every row and the index buys
#' nothing. The complement of the intersects list is the same answer.
#'
#' @noRd
st_disjoint <- function(x, y, sparse = TRUE, ...) {
  x <- as_ga(x)
  y <- as_ga(y)
  all_rows <- seq_len(length(y))

  hits <- lapply(as.vector(ga_sparse_intersects(x, y)), function(hit) {
    if (is.null(hit)) all_rows else setdiff(all_rows, as.integer(hit))
  })

  if (isTRUE(sparse)) {
    return(hits)
  }
  predicate_out(hits, length(x), length(y), sparse = FALSE)
}

#' @noRd
st_relate <- function(x, y, pattern = NA_character_, ...) {
  out <- ga_relate(as_ga(x), as_ga(y))
  if (is.na(pattern)) {
    return(out)
  }
  as.vector(out) == pattern
}

#' @noRd
st_is_within_distance <- function(x, y, dist, sparse = TRUE, ...) {
  x <- as_ga(x)
  y <- as_ga(y)
  predicate_out(
    ga_sparse_dwithin(x, y, dist),
    length(x),
    length(y),
    sparse
  )
}

#' @noRd
st_nearest_feature <- function(x, y, ...) {
  x <- as_ga(x)
  y <- if (missing(y)) x else as_ga(y)

  vapply(
    as.vector(ga_sparse_knn(x, y, k = 1)),
    function(hit) {
      if (is.null(hit) || nrow(hit) == 0L) {
        NA_integer_
      } else {
        as.integer(hit$row[1])
      }
    },
    integer(1)
  )
}

#' @noRd
st_combine <- function(x) {
  ga_out(ga_collect_agg(as_ga(x)))
}

#' A geometry is simple when it does not cross itself
#'
#' `ga_self_intersections()` gives the crossing points, so an empty result is
#' the answer.
#'
#' @noRd
st_is_simple <- function(x) {
  ga_is_empty(ga_self_intersections(as_ga(x)))
}

#' @noRd
st_line_interpolate <- function(line, dist, normalized = FALSE) {
  ga_out(ga_interpolate_point(
    as_ga(line),
    dist,
    metric = "euclidean",
    measure = if (isTRUE(normalized)) "ratio" else "distance",
    from = "start"
  ))
}

#' @noRd
st_line_project <- function(line, point, ...) {
  ga_line_locate_point(as_ga(line), as_ga(point))
}

#' The non-generic sf functions mask_sf() installs
#'
#' @noRd
sf_masked <- c(
  "st_combine",
  "st_contains",
  "st_contains_properly",
  "st_covered_by",
  "st_covers",
  "st_crosses",
  "st_dimension",
  "st_disjoint",
  "st_distance",
  "st_equals",
  "st_is_empty",
  "st_is_simple",
  "st_is_within_distance",
  "st_length",
  "st_line_interpolate",
  "st_line_project",
  "st_nearest_feature",
  "st_overlaps",
  "st_perimeter",
  "st_relate",
  "st_touches",
  "st_within"
)

#' Put the sf lookalikes on the search path
#'
#' Attaches the geoarrowrs versions of the sf functions sf does not make
#' generic, such as [st_distance()] and [st_within()], under sf's own names and
#' argument names. Remove them again with `detach("geoarrowrs:sf")`.
#'
#' @details
#' They take and return the GeoArrow representation rather than `sfc`, so
#' nothing is converted into an sf object on the way through.
#'
#' The generics are not attached, because masking one would shadow sf's own
#' dispatch. Those are S3 methods registered on `nanoarrow_vctr`, which
#' `geoarrow_vctr` extends, so `st_area()` and the rest already reach this
#' package through sf's generic whether or not this is called.
#'
#' @returns The attached environment, invisibly.
#' @export
#' @examplesIf requireNamespace("sf", quietly = TRUE) && requireNamespace("geoarrow", quietly = TRUE)
#' nc <- as.data.frame(read_shapefile(
#'   system.file("shape/nc.shp", package = "sf")
#' ))
#' geom <- geoarrow::as_geoarrow_vctr(nc$geometry)
#'
#' mask_sf()
#' as.vector(st_length(geom))[1:3]
#' as.vector(st_within(ga_centroid(geom), geom))[1:3]
#' detach("geoarrowrs:sf")
mask_sf <- function() {
  if ("geoarrowrs:sf" %in% search()) {
    detach("geoarrowrs:sf", character.only = TRUE)
  }

  ns <- asNamespace("geoarrowrs")
  fns <- lapply(rlang::set_names(sf_masked), get, envir = ns)

  invisible(attach(fns, name = "geoarrowrs:sf", warn.conflicts = FALSE))
}
