# Changelog

## geoarrowrs (development version)

- [`ga_dbscan()`](https://josiahparry.github.io/geoarrowrs/reference/ga_dbscan.md),
  [`ga_kmeans()`](https://josiahparry.github.io/geoarrowrs/reference/ga_kmeans.md),
  and
  [`ga_outlier_scores()`](https://josiahparry.github.io/geoarrowrs/reference/ga_outlier_scores.md)
  now cluster over the whole array and return one value per row, rather
  than clustering within each row and returning a list per row. A point
  array of 10,000 locations gives 10,000 labels, which is what these are
  for. `eps`, `min_points`, `k`, and `k_neighbours` are single values
  now, since the operation spans the array rather than a row. A row that
  is not a single point takes no part and comes back null. They are no
  longer registered as Arrow kernels, because a kernel sees one batch at
  a time and would cluster each batch separately.
- `RTree$new()` takes a `sort` argument, either `"hilbert"` (the
  default) or `"str"`, choosing how the tree is packed.
- `RTree$query()` looks up a whole array at once and returns one list of
  candidate rows per element, which is the shape a spatial join needs.
  It accepts any GeoArrow array and reduces it to bounding boxes itself,
  so points, polygons and box arrays all work. The scalar `$search()` is
  unchanged.
- Added
  [`ga_envelope()`](https://josiahparry.github.io/geoarrowrs/reference/ga_envelope.md),
  which returns the axis aligned bounding box of each geometry as a
  GeoArrow box array. Unlike
  [`ga_bounding_rect()`](https://josiahparry.github.io/geoarrowrs/reference/ga_bounding_rect.md)
  it passes a box array straight through instead of recomputing it, so
  it is safe to call before a query without paying for it twice.
- The parallel paths use every core by default. Set
  `options(geoarrowrs.thread_pool = n)` for a cap, which takes effect
  immediately rather than at load. Under `R CMD check` the cap defaults
  to two, since CRAN sets `_R_CHECK_LIMIT_CORES_` and asks for no more
  than that; `OMP_THREAD_LIMIT` is honoured as a ceiling too. Setting
  the option yourself overrides both.
- Casting between geometry types now runs in parallel, in rayon jobs of
  at least 8192 rows. Parsing WKB is most of the cost of reading a plain
  Parquet geometry column, and Arrow cannot thread a user-defined
  function because it has to call back into R on the main thread, so the
  parallelism has to live inside the Rust. On ten cores a 3M row WKB to
  point cast goes from 2.0s to 0.32s, and a distance over 6M rows from
  9.6s to 3.3s.
- The pairwise distance and bearing functions now error on a length
  mismatch instead of silently truncating.
  [`ga_dist_euclidean_pairwise()`](https://josiahparry.github.io/geoarrowrs/reference/dist_pairwise.md)
  given two origins and one destination returned a single value,
  breaking the length preserving guarantee without saying anything. All
  eleven affected functions across `distance/` and `bearing/` now check,
  and `check_pair_len()` moved to the crate root so `interpolate_point/`
  shares it.
- Added
  [`ga_as_point()`](https://josiahparry.github.io/geoarrowrs/reference/ga_as_point.md),
  [`ga_as_linestring()`](https://josiahparry.github.io/geoarrowrs/reference/ga_as_point.md),
  [`ga_as_polygon()`](https://josiahparry.github.io/geoarrowrs/reference/ga_as_point.md),
  [`ga_as_multipoint()`](https://josiahparry.github.io/geoarrowrs/reference/ga_as_point.md),
  [`ga_as_multilinestring()`](https://josiahparry.github.io/geoarrowrs/reference/ga_as_point.md),
  and
  [`ga_as_multipolygon()`](https://josiahparry.github.io/geoarrowrs/reference/ga_as_point.md),
  which cast a bare WKB column or any GeoArrow array to one named
  geometry type. Because the result type is fixed by the name rather
  than by the data, these are the only geometry producing casts Arrow
  can run inside a query, so a plain Parquet column can be turned into
  native GeoArrow once and computed on after that.
- Registered Arrow kernels now accept a bare `binary` or `large_binary`
  WKB column, the kind plain Parquet writes, so
  `mutate(d = ga_dist_euclidean_pairwise(pickup, dropoff))` runs on a
  Dataset without converting the column first. A geometry result from a
  WKB input comes back as WKB, since the kernel cannot declare a type
  that depends on what the column turns out to hold.
- Loading the package now registers the Arrow kernels for CRS-less data
  automatically, so `mutate(a = ga_unsigned_area(geometry))` works on a
  `Table` or `Dataset` with no setup. Data carrying a CRS still needs
  `register_geoarrow_udfs(crs = ...)`. Set
  `options(geoarrowrs.register_udfs = FALSE)` to skip it and avoid
  loading arrow at all.
- Added
  [`ga_from_wkb()`](https://josiahparry.github.io/geoarrowrs/reference/ga_from_wkb.md),
  which reads a bare `binary` WKB column, the kind plain Parquet writes,
  into the narrowest GeoArrow type that fits it. Functions needing a
  specific geometry type, such as
  [`ga_dist_euclidean_pairwise()`](https://josiahparry.github.io/geoarrowrs/reference/dist_pairwise.md),
  could not read those columns at all.
- Added
  [`ga_xy()`](https://josiahparry.github.io/geoarrowrs/reference/ga_xy.md),
  which pairs two numeric vectors or float64 arrays into a GeoArrow
  point array. Either argument may be length 1 and is recycled, a row
  where either coordinate is `NA` gives a null point, and `crs` is
  recorded in the array metadata verbatim without reprojecting anything.
- Every geometry function now carries a `ga_` prefix:
  [`ga_centroid()`](https://josiahparry.github.io/geoarrowrs/reference/ga_centroid.md),
  [`ga_intersects()`](https://josiahparry.github.io/geoarrowrs/reference/topology.md),
  [`ga_simplify()`](https://josiahparry.github.io/geoarrowrs/reference/ga_simplify.md).
  Bare names masked
  [`base::within()`](https://rdrr.io/r/base/with.html),
  [`stats::kmeans()`](https://rdrr.io/r/stats/kmeans.html),
  [`graphics::lines()`](https://rdrr.io/r/graphics/lines.html),
  [`dplyr::contains()`](https://tidyselect.r-lib.org/reference/starts_with.html),
  and `purrr::flatten()`. An `st_` prefix was considered and rejected,
  because 19 of the names collide head on with sf and return a different
  type. The readers
  [`read_shapefile()`](https://josiahparry.github.io/geoarrowrs/reference/read_shapefile.md),
  [`read_geojson()`](https://josiahparry.github.io/geoarrowrs/reference/read_geojson.md),
  and
  [`read_flatgeobuf()`](https://josiahparry.github.io/geoarrowrs/reference/read_flatgeobuf.md),
  the `RTree` class, and
  [`register_geoarrow_udfs()`](https://josiahparry.github.io/geoarrowrs/reference/register_geoarrow_udfs.md)
  keep their names. Registered Arrow kernels take the prefix too, so
  `mutate(a = ga_unsigned_area(geometry))` is the same name in and out
  of the engine.
- Added
  [`ga_line_intersection()`](https://josiahparry.github.io/geoarrowrs/reference/ga_line_intersection.md)
  and
  [`ga_self_intersections()`](https://josiahparry.github.io/geoarrowrs/reference/ga_self_intersections.md),
  the latter using the Bentley-Ottmann sweep to locate exactly where a
  geometry crosses itself.
- Added
  [`ga_affine_transform()`](https://josiahparry.github.io/geoarrowrs/reference/ga_affine_transform.md),
  the general form behind the other affine ops, taking six recyclable
  coefficients so a different transform can apply to every row.
- Added
  [`ga_dbscan()`](https://josiahparry.github.io/geoarrowrs/reference/ga_dbscan.md),
  [`ga_kmeans()`](https://josiahparry.github.io/geoarrowrs/reference/ga_kmeans.md),
  and
  [`ga_outlier_scores()`](https://josiahparry.github.io/geoarrowrs/reference/ga_outlier_scores.md)
  for clustering the points within each row, plus
  [`ga_simplify_idx()`](https://josiahparry.github.io/geoarrowrs/reference/ga_simplify_idx.md)
  and
  [`ga_simplify_vw_idx()`](https://josiahparry.github.io/geoarrowrs/reference/ga_simplify_idx.md),
  which return the coordinate positions simplification keeps rather than
  the simplified geometry.
- Added
  [`ga_coords()`](https://josiahparry.github.io/geoarrowrs/reference/ga_coords.md),
  [`ga_exterior_coords()`](https://josiahparry.github.io/geoarrowrs/reference/ga_coords.md),
  [`ga_n_coords()`](https://josiahparry.github.io/geoarrowrs/reference/ga_n_coords.md),
  and
  [`ga_lines()`](https://josiahparry.github.io/geoarrowrs/reference/ga_lines.md)
  for walking a geometry’s vertices and segments, plus
  [`ga_is_valid()`](https://josiahparry.github.io/geoarrowrs/reference/validation.md)
  and
  [`ga_validation_error()`](https://josiahparry.github.io/geoarrowrs/reference/validation.md).
- Added an `RTree` spatial index built on the `geo-index` crate, with
  `$search()` for bounding box queries and `$neighbors()` for nearest
  rows. Queries return candidates to confirm with an exact predicate.
- Added
  [`ga_voronoi_cells()`](https://josiahparry.github.io/geoarrowrs/reference/ga_voronoi_cells.md)
  and
  [`ga_voronoi_edges()`](https://josiahparry.github.io/geoarrowrs/reference/ga_voronoi_edges.md),
  returning one multipolygon of cells or multilinestring of edges per
  input geometry. Both take a `clip` mode and an optional `boundary`
  polygon to cut the diagram to a study area.
- Added
  [`ga_orient()`](https://josiahparry.github.io/geoarrowrs/reference/ga_orient.md),
  [`ga_winding_order()`](https://josiahparry.github.io/geoarrowrs/reference/ga_winding_order.md),
  [`ga_is_ccw()`](https://josiahparry.github.io/geoarrowrs/reference/ga_is_ccw.md),
  and
  [`ga_is_cw()`](https://josiahparry.github.io/geoarrowrs/reference/ga_is_ccw.md).
  A multi part geometry reports a winding only when all of its parts
  agree.
- Added
  [`register_geoarrow_udfs()`](https://josiahparry.github.io/geoarrowrs/reference/register_geoarrow_udfs.md),
  which registers geoarrowrs functions as Arrow scalar kernels so they
  run inside `dplyr` verbs on a `Table` or `Dataset` rather than pulling
  the geometry into R. It registers for a CRS rather than for a table:
  one call covers every GeoArrow geometry type, and functions taking
  numeric or option arguments, such as
  [`ga_simplify()`](https://josiahparry.github.io/geoarrowrs/reference/ga_simplify.md),
  [`ga_densify()`](https://josiahparry.github.io/geoarrowrs/reference/ga_densify.md),
  and
  [`ga_buffer()`](https://josiahparry.github.io/geoarrowrs/reference/ga_buffer.md),
  are registered alongside the geometry-only ones. 101 functions in
  total.
- Added the topological predicates
  [`ga_contains()`](https://josiahparry.github.io/geoarrowrs/reference/topology.md),
  [`ga_contains_properly()`](https://josiahparry.github.io/geoarrowrs/reference/topology.md),
  [`ga_within()`](https://josiahparry.github.io/geoarrowrs/reference/topology.md),
  [`ga_covers()`](https://josiahparry.github.io/geoarrowrs/reference/topology.md),
  [`ga_covered_by()`](https://josiahparry.github.io/geoarrowrs/reference/topology.md),
  [`ga_intersects()`](https://josiahparry.github.io/geoarrowrs/reference/topology.md),
  [`ga_disjoint()`](https://josiahparry.github.io/geoarrowrs/reference/topology.md),
  [`ga_touches()`](https://josiahparry.github.io/geoarrowrs/reference/topology.md),
  [`ga_crosses()`](https://josiahparry.github.io/geoarrowrs/reference/topology.md),
  [`ga_overlaps()`](https://josiahparry.github.io/geoarrowrs/reference/topology.md),
  and
  [`ga_equals_topo()`](https://josiahparry.github.io/geoarrowrs/reference/topology.md),
  plus
  [`ga_relate()`](https://josiahparry.github.io/geoarrowrs/reference/ga_relate.md)
  for the DE-9IM string,
  [`ga_dimension()`](https://josiahparry.github.io/geoarrowrs/reference/ga_dimension.md),
  [`ga_boundary_dimension()`](https://josiahparry.github.io/geoarrowrs/reference/ga_dimension.md),
  [`ga_is_empty()`](https://josiahparry.github.io/geoarrowrs/reference/ga_is_empty.md),
  and
  [`ga_coordinate_position()`](https://josiahparry.github.io/geoarrowrs/reference/ga_coordinate_position.md).
- Added
  [`ga_boolean_intersection()`](https://josiahparry.github.io/geoarrowrs/reference/boolean_ops.md),
  [`ga_boolean_union()`](https://josiahparry.github.io/geoarrowrs/reference/boolean_ops.md),
  [`ga_boolean_difference()`](https://josiahparry.github.io/geoarrowrs/reference/boolean_ops.md),
  [`ga_boolean_xor()`](https://josiahparry.github.io/geoarrowrs/reference/boolean_ops.md),
  and
  [`ga_unary_union()`](https://josiahparry.github.io/geoarrowrs/reference/ga_unary_union.md).
- Added
  [`ga_closest_point()`](https://josiahparry.github.io/geoarrowrs/reference/ga_closest_point.md),
  [`ga_closest_point_haversine()`](https://josiahparry.github.io/geoarrowrs/reference/ga_closest_point.md),
  [`ga_interior_point()`](https://josiahparry.github.io/geoarrowrs/reference/ga_interior_point.md),
  [`ga_is_convex()`](https://josiahparry.github.io/geoarrowrs/reference/ga_is_convex.md),
  and
  [`ga_line_locate_point()`](https://josiahparry.github.io/geoarrowrs/reference/ga_line_locate_point.md).
  [`ga_interior_point()`](https://josiahparry.github.io/geoarrowrs/reference/ga_interior_point.md)
  always lands on the geometry, unlike
  [`ga_centroid()`](https://josiahparry.github.io/geoarrowrs/reference/ga_centroid.md).
- Added
  [`read_shapefile()`](https://josiahparry.github.io/geoarrowrs/reference/read_shapefile.md),
  [`read_geojson()`](https://josiahparry.github.io/geoarrowrs/reference/read_geojson.md),
  and
  [`read_flatgeobuf()`](https://josiahparry.github.io/geoarrowrs/reference/read_flatgeobuf.md),
  each returning a record batch stream of the attribute columns followed
  by a `geometry` column.
  [`read_flatgeobuf()`](https://josiahparry.github.io/geoarrowrs/reference/read_flatgeobuf.md)
  streams and takes an optional `bbox` that uses the file’s spatial
  index.
- Added
  [`ga_cast_geometry()`](https://josiahparry.github.io/geoarrowrs/reference/ga_cast_geometry.md)
  and
  [`ga_downcast_geometry()`](https://josiahparry.github.io/geoarrowrs/reference/ga_downcast_geometry.md)
  for converting between GeoArrow geometry types, and
  [`ga_explode()`](https://josiahparry.github.io/geoarrowrs/reference/ga_explode.md)/[`ga_flatten()`](https://josiahparry.github.io/geoarrowrs/reference/ga_flatten.md)
  for splitting multi-part geometries into their parts and collapsing
  them again.
  [`ga_explode()`](https://josiahparry.github.io/geoarrowrs/reference/ga_explode.md)
  preserves length, returning one array of parts per row.
- Added
  [`ga_triangulate_earcut()`](https://josiahparry.github.io/geoarrowrs/reference/ga_triangulate_earcut.md)
  and
  [`ga_triangulate_delaunay()`](https://josiahparry.github.io/geoarrowrs/reference/ga_triangulate_delaunay.md),
  returning one multipolygon of triangles per input geometry.
- Added
  [`ga_convex_hull()`](https://josiahparry.github.io/geoarrowrs/reference/ga_convex_hull.md),
  [`ga_concave_hull()`](https://josiahparry.github.io/geoarrowrs/reference/ga_concave_hull.md),
  and
  [`ga_extremes()`](https://josiahparry.github.io/geoarrowrs/reference/ga_extremes.md),
  plus the affine operations
  [`ga_translate()`](https://josiahparry.github.io/geoarrowrs/reference/ga_translate.md),
  [`ga_rotate_around_centroid()`](https://josiahparry.github.io/geoarrowrs/reference/ga_rotate_around_centroid.md),
  [`ga_rotate_around_center()`](https://josiahparry.github.io/geoarrowrs/reference/ga_rotate_around_center.md),
  [`ga_scale_xy()`](https://josiahparry.github.io/geoarrowrs/reference/ga_scale_xy.md),
  [`ga_skew()`](https://josiahparry.github.io/geoarrowrs/reference/ga_skew.md),
  and
  [`ga_skew_xy()`](https://josiahparry.github.io/geoarrowrs/reference/ga_skew_xy.md).
- Added
  [`ga_to_degrees()`](https://josiahparry.github.io/geoarrowrs/reference/ga_to_degrees.md)
  and
  [`ga_to_radians()`](https://josiahparry.github.io/geoarrowrs/reference/ga_to_radians.md).
- Numeric arguments now accept a plain R numeric vector as well as an
  Arrow array, so `ga_simplify(x, 0.01)` works without wrapping the
  value.
- Geometry arguments now accept any GeoArrow array rather than only a
  mixed `geometry` array.
  [`ga_centroid()`](https://josiahparry.github.io/geoarrowrs/reference/ga_centroid.md),
  [`ga_signed_area()`](https://josiahparry.github.io/geoarrowrs/reference/area.md),
  [`ga_buffer()`](https://josiahparry.github.io/geoarrowrs/reference/ga_buffer.md)
  and ten others previously errored on the multipolygon arrays the
  readers produce.
- Empty points no longer crash. Every geo-backed function panicked on an
  array containing one, because `to_geometry()` cannot represent an
  empty point.
- [`ga_densify()`](https://josiahparry.github.io/geoarrowrs/reference/ga_densify.md),
  [`ga_interpolate_point()`](https://josiahparry.github.io/geoarrowrs/reference/ga_interpolate_point.md),
  [`ga_point_at_distance_between()`](https://josiahparry.github.io/geoarrowrs/reference/interpolate_between.md),
  [`ga_point_at_ratio_between()`](https://josiahparry.github.io/geoarrowrs/reference/interpolate_between.md),
  [`ga_points_along_line()`](https://josiahparry.github.io/geoarrowrs/reference/ga_points_along_line.md),
  [`ga_dest_euclidean()`](https://josiahparry.github.io/geoarrowrs/reference/destination.md),
  [`ga_dest_haversine()`](https://josiahparry.github.io/geoarrowrs/reference/destination.md),
  and
  [`ga_dest_geodesic()`](https://josiahparry.github.io/geoarrowrs/reference/destination.md)
  are now exported. They were written but never registered.
- [`ga_point_at_ratio_between()`](https://josiahparry.github.io/geoarrowrs/reference/interpolate_between.md)
  returned `NULL` instead of its result, and the `interpolate_point`
  functions ignored recycling despite documenting it.
