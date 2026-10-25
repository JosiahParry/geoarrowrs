# Changelog

## geoarrowrs (development version)

- [`ga_translate()`](https://josiahparry.github.io/geoarrowrs/reference/ga_translate.md)
  and
  [`ga_jitter()`](https://josiahparry.github.io/geoarrowrs/reference/ga_jitter.md)
  keep an empty point empty instead of failing.
- Added
  [`ga_jitter()`](https://josiahparry.github.io/geoarrowrs/reference/ga_jitter.md),
  which moves each geometry by a small random offset like
  [`sf::st_jitter()`](https://r-spatial.github.io/sf/reference/st_jitter.html),
  and
  [`mask_sf()`](https://josiahparry.github.io/geoarrowrs/reference/mask_sf.md)
  attaches
  [`st_jitter()`](https://r-spatial.github.io/sf/reference/st_jitter.html).
- Added
  [`ga_interpolate_aw()`](https://josiahparry.github.io/geoarrowrs/reference/ga_interpolate_aw.md),
  which interpolates extensive and intensive values from one set of
  polygons onto another by area, and
  [`sf::st_interpolate_aw()`](https://r-spatial.github.io/sf/reference/interpolate_aw.html)
  works on a data frame with a GeoArrow geometry column.
- Added
  [`ga_make_grid()`](https://josiahparry.github.io/geoarrowrs/reference/ga_make_grid.md),
  which builds a square grid of cells, centres, or corners like
  [`sf::st_make_grid()`](https://r-spatial.github.io/sf/reference/st_make_grid.html),
  and
  [`mask_sf()`](https://josiahparry.github.io/geoarrowrs/reference/mask_sf.md)
  attaches
  [`st_make_grid()`](https://r-spatial.github.io/sf/reference/st_make_grid.html).
- Added
  [`ga_sparse_equals_exact()`](https://josiahparry.github.io/geoarrowrs/reference/ga_sparse_equals_exact.md),
  the sparse form of
  [`ga_equals_exact()`](https://josiahparry.github.io/geoarrowrs/reference/ga_equals_exact.md),
  and
  [`mask_sf()`](https://josiahparry.github.io/geoarrowrs/reference/mask_sf.md)
  attaches
  [`st_equals_exact()`](https://r-spatial.github.io/sf/reference/geos_binary_pred.html).
- Added
  [`ga_make_valid()`](https://josiahparry.github.io/geoarrowrs/reference/ga_make_valid.md),
  which repairs invalid polygons and multipolygons, and
  [`sf::st_make_valid()`](https://r-spatial.github.io/sf/reference/valid.html)
  and
  [`sdf::st_make_valid()`](https://rdrr.io/pkg/sdf/man/processing.html)
  work on a `geoarrow_vctr`.
- Added
  [`ga_linestring()`](https://josiahparry.github.io/geoarrowrs/reference/ga_linestring.md),
  [`ga_multipoint()`](https://josiahparry.github.io/geoarrowrs/reference/ga_multipoint.md),
  [`ga_multilinestring()`](https://josiahparry.github.io/geoarrowrs/reference/ga_multilinestring.md),
  and
  [`ga_multipolygon()`](https://josiahparry.github.io/geoarrowrs/reference/ga_multipolygon.md),
  which build geometries from coordinate columns and ids like
  [`ga_polygon()`](https://josiahparry.github.io/geoarrowrs/reference/ga_polygon.md).
- [`ga_point()`](https://josiahparry.github.io/geoarrowrs/reference/ga_point.md)
  takes `z` and `m`, which add those dimensions to the points.
- [`ga_collect_agg()`](https://josiahparry.github.io/geoarrowrs/reference/ga_collect_agg.md)
  gathers a point, line, or polygon array into multipoints,
  multilinestrings, or multipolygons, and only gathers mixed or untyped
  arrays into geometry collections.
- Added
  [`ga_make_polygon()`](https://josiahparry.github.io/geoarrowrs/reference/ga_make_polygon.md),
  which builds a polygon from each shell linestring and the holes on its
  row.
- Added
  [`ga_make_line_agg()`](https://josiahparry.github.io/geoarrowrs/reference/ga_make_line_agg.md),
  which joins the points that share each label of `by` into a
  linestring.
- Added
  [`ga_polygon()`](https://josiahparry.github.io/geoarrowrs/reference/ga_polygon.md),
  which builds polygons from coordinate columns and polygon and ring
  ids, like `sfheaders::sf_polygon()`.
- [`ga_collect_agg()`](https://josiahparry.github.io/geoarrowrs/reference/ga_collect_agg.md)
  accepts a numeric vector for `by`.
- Added
  [`ga_str_order()`](https://josiahparry.github.io/geoarrowrs/reference/ga_str_order.md),
  which orders rows by sort-tile-recursive packing so that nearby
  geometries sit together.
- Added
  [`ga_hilbert_order()`](https://josiahparry.github.io/geoarrowrs/reference/ga_hilbert_order.md),
  which orders rows along a Hilbert curve so that nearby geometries sit
  together.
- `RTree` and `KDTree` gain
  [`write()`](https://rdrr.io/r/base/write.html) and `read()`, which
  save an index to a file and open it again, optionally memory mapped.
- Added
  [`ga_make_envelope()`](https://josiahparry.github.io/geoarrowrs/reference/ga_make_envelope.md),
  which builds a rectangle polygon from `xmin`, `ymin`, `xmax`, and
  `ymax`.
- [`sf::st_reverse()`](https://r-spatial.github.io/sf/reference/geos_unary.html),
  [`sf::st_collection_extract()`](https://r-spatial.github.io/sf/reference/st_collection_extract.html),
  and
  [`sf::st_is()`](https://r-spatial.github.io/sf/reference/st_is.html)
  work on a `geoarrow_vctr`, and
  [`mask_sf()`](https://josiahparry.github.io/geoarrowrs/reference/mask_sf.md)
  attaches
  [`st_geometry_type()`](https://r-spatial.github.io/sf/reference/st_geometry_type.html).
- [`ga_translate()`](https://josiahparry.github.io/geoarrowrs/reference/ga_translate.md)
  accepts any geometry type, including multipoints, geometry
  collections, and WKB.
- [`sf::st_bbox()`](https://r-spatial.github.io/sf/reference/st_bbox.html)
  works on a `geoarrow_vctr`, returning one box over every row.
- Added
  [`ga_collection_extract()`](https://josiahparry.github.io/geoarrowrs/reference/ga_collection_extract.md),
  which gathers the points, lines, or polygons of each geometry into one
  multi geometry per row, or a null where there are none.
- Added
  [`ga_reverse()`](https://josiahparry.github.io/geoarrowrs/reference/ga_reverse.md),
  which reverses the coordinate order of every line and ring.
- Removed `ga_bounding_rect()`, which duplicated
  [`ga_envelope()`](https://josiahparry.github.io/geoarrowrs/reference/ga_envelope.md).
- Added
  [`ga_equals_exact()`](https://josiahparry.github.io/geoarrowrs/reference/ga_equals_exact.md).
- Added
  [`ga_perimeter_euclidean()`](https://josiahparry.github.io/geoarrowrs/reference/ga_perimeter_euclidean.md).
- Added
  [`ga_geometry_type()`](https://josiahparry.github.io/geoarrowrs/reference/ga_geometry_type.md)
  and
  [`ga_is()`](https://josiahparry.github.io/geoarrowrs/reference/ga_is.md).
- A `geoarrow_vctr` is now an sdf geometry backend, implementing sdf’s
  generics so `sdf_filter()` and `sdf_join()` run on Arrow.
- Added
  [`mask_sf()`](https://josiahparry.github.io/geoarrowrs/reference/mask_sf.md),
  which attaches sf’s names and argument names for the GeoArrow
  representation, alongside S3 methods on `geoarrow_vctr` for the ones
  sf makes generic.
- [`ga_collect_agg()`](https://josiahparry.github.io/geoarrowrs/reference/ga_collect_agg.md)
  takes `by`, an unsorted key of one label per row, and groups the rows
  itself instead of requiring an
  [`arrange()`](https://dplyr.tidyverse.org/reference/arrange.html)
  first.
- Added
  [`ga_set_thread_pool()`](https://josiahparry.github.io/geoarrowrs/reference/ga_set_thread_pool.md),
  which sets the thread cap used by the parallel paths.
- [`ga_sparse_knn()`](https://josiahparry.github.io/geoarrowrs/reference/ga_sparse_knn.md)
  and
  [`ga_sparse_dwithin()`](https://josiahparry.github.io/geoarrowrs/reference/ga_sparse_dwithin.md)
  take `metric`, one of `"euclidean"`, `"haversine"`, `"geodesic"`, or
  `"rhumb"`.
- Added
  [`ga_filter()`](https://josiahparry.github.io/geoarrowrs/reference/ga_filter.md),
  which keeps the rows of one data frame that relate to any row of
  another.
- Added
  [`ga_sparse_pairs()`](https://josiahparry.github.io/geoarrowrs/reference/ga_sparse_pairs.md),
  which expands a sparse predicate’s list result into one row per pair.
- Added
  [`ga_cross_distance()`](https://josiahparry.github.io/geoarrowrs/reference/ga_cross_distance.md),
  which measures every row of `x` against every row of `y`, returning
  one list of distances per row.
- Added
  [`ga_sparse_knn()`](https://josiahparry.github.io/geoarrowrs/reference/ga_sparse_knn.md)
  and
  [`ga_knn_join()`](https://josiahparry.github.io/geoarrowrs/reference/ga_knn_join.md),
  a nearest neighbour search and the join over it.
- Materialising geometries now runs in parallel throughout the package.
- [`ga_collect_agg()`](https://josiahparry.github.io/geoarrowrs/reference/ga_collect_agg.md)
  takes `sizes`, the number of rows in each group, and returns one
  collection per group instead of one for the whole array.
- Reading geometries splits into more, smaller rayon tasks, sized
  against the thread pool.
- Added
  [`ga_sparse_dwithin()`](https://josiahparry.github.io/geoarrowrs/reference/ga_sparse_dwithin.md),
  which finds the rows of `y` within a distance of each row of `x`.
- A point on either side of a sparse predicate is now answered from
  where the point lies rather than from a DE-9IM matrix.
- The topological predicates node each geometry once rather than once
  per comparison.
- `RTree` takes arrays everywhere, matching `KDTree`.
- Added
  [`ga_collect_agg()`](https://josiahparry.github.io/geoarrowrs/reference/ga_collect_agg.md),
  which gathers a whole array into one geometry collection of length 1.
- [`ga_dist_euclidean_pairwise()`](https://josiahparry.github.io/geoarrowrs/reference/dist_pairwise.md)
  and
  [`ga_dist_hausdorff_pairwise()`](https://josiahparry.github.io/geoarrowrs/reference/ga_dist_hausdorff_pairwise.md)
  measure between geometries of any type, not just points.
- Added
  [`ga_make_line()`](https://josiahparry.github.io/geoarrowrs/reference/ga_make_line.md),
  which joins each point of one array to the matching point of another.
- Added
  [`ga_x()`](https://josiahparry.github.io/geoarrowrs/reference/ga_x.md)
  and
  [`ga_y()`](https://josiahparry.github.io/geoarrowrs/reference/ga_x.md),
  which read the coordinates of a point array as doubles.
- The distance and bearing functions recycle a length 1 `dest`.
- Added
  [`ga_join()`](https://josiahparry.github.io/geoarrowrs/reference/ga_join.md),
  which attaches the columns of one data frame to each row of another it
  relates to spatially.
- Added the sparse predicates
  [`ga_sparse_intersects()`](https://josiahparry.github.io/geoarrowrs/reference/sparse.md),
  [`ga_sparse_contains()`](https://josiahparry.github.io/geoarrowrs/reference/sparse.md),
  [`ga_sparse_contains_properly()`](https://josiahparry.github.io/geoarrowrs/reference/sparse.md),
  [`ga_sparse_within()`](https://josiahparry.github.io/geoarrowrs/reference/sparse.md),
  [`ga_sparse_covers()`](https://josiahparry.github.io/geoarrowrs/reference/sparse.md),
  [`ga_sparse_covered_by()`](https://josiahparry.github.io/geoarrowrs/reference/sparse.md),
  [`ga_sparse_touches()`](https://josiahparry.github.io/geoarrowrs/reference/sparse.md),
  [`ga_sparse_crosses()`](https://josiahparry.github.io/geoarrowrs/reference/sparse.md),
  [`ga_sparse_overlaps()`](https://josiahparry.github.io/geoarrowrs/reference/sparse.md),
  and
  [`ga_sparse_equals_topo()`](https://josiahparry.github.io/geoarrowrs/reference/sparse.md).
- Added `KDTree`, a k-d tree over a point array.
- Added `RTree`, a packed Hilbert R-tree over each geometry’s bounding
  box.
- Added
  [`ga_envelope()`](https://josiahparry.github.io/geoarrowrs/reference/ga_envelope.md),
  which returns the axis aligned bounding box of each geometry.
- Added
  [`ga_dbscan()`](https://josiahparry.github.io/geoarrowrs/reference/ga_dbscan.md),
  [`ga_kmeans()`](https://josiahparry.github.io/geoarrowrs/reference/ga_kmeans.md),
  [`ga_outlier_scores()`](https://josiahparry.github.io/geoarrowrs/reference/ga_outlier_scores.md),
  [`ga_simplify_idx()`](https://josiahparry.github.io/geoarrowrs/reference/ga_simplify_idx.md),
  and
  [`ga_simplify_vw_idx()`](https://josiahparry.github.io/geoarrowrs/reference/ga_simplify_idx.md).
- Added
  [`ga_as_point()`](https://josiahparry.github.io/geoarrowrs/reference/ga_as_point.md),
  [`ga_as_linestring()`](https://josiahparry.github.io/geoarrowrs/reference/ga_as_point.md),
  [`ga_as_polygon()`](https://josiahparry.github.io/geoarrowrs/reference/ga_as_point.md),
  [`ga_as_multipoint()`](https://josiahparry.github.io/geoarrowrs/reference/ga_as_point.md),
  [`ga_as_multilinestring()`](https://josiahparry.github.io/geoarrowrs/reference/ga_as_point.md),
  and
  [`ga_as_multipolygon()`](https://josiahparry.github.io/geoarrowrs/reference/ga_as_point.md).
- Added
  [`ga_from_wkb()`](https://josiahparry.github.io/geoarrowrs/reference/ga_from_wkb.md),
  which reads a bare `binary` WKB column into the narrowest GeoArrow
  type that fits it.
- Added
  [`ga_point()`](https://josiahparry.github.io/geoarrowrs/reference/ga_point.md),
  which pairs two numeric vectors or float64 arrays into a GeoArrow
  point array.
- Added
  [`register_geoarrow_udfs()`](https://josiahparry.github.io/geoarrowrs/reference/register_geoarrow_udfs.md),
  which registers geoarrowrs functions as Arrow scalar kernels.
- Loading the package registers the Arrow kernels for CRS-less data
  automatically.
- Registered Arrow kernels accept a bare `binary` or `large_binary` WKB
  column.
- Added
  [`ga_line_intersection()`](https://josiahparry.github.io/geoarrowrs/reference/ga_line_intersection.md)
  and
  [`ga_self_intersections()`](https://josiahparry.github.io/geoarrowrs/reference/ga_self_intersections.md).
- Added
  [`ga_affine_transform()`](https://josiahparry.github.io/geoarrowrs/reference/ga_affine_transform.md).
- Added
  [`ga_coords()`](https://josiahparry.github.io/geoarrowrs/reference/ga_coords.md),
  [`ga_exterior_coords()`](https://josiahparry.github.io/geoarrowrs/reference/ga_coords.md),
  [`ga_n_coords()`](https://josiahparry.github.io/geoarrowrs/reference/ga_n_coords.md),
  [`ga_lines()`](https://josiahparry.github.io/geoarrowrs/reference/ga_lines.md),
  [`ga_is_valid()`](https://josiahparry.github.io/geoarrowrs/reference/validation.md),
  and
  [`ga_validation_error()`](https://josiahparry.github.io/geoarrowrs/reference/validation.md).
- Added
  [`ga_voronoi_cells()`](https://josiahparry.github.io/geoarrowrs/reference/ga_voronoi_cells.md)
  and
  [`ga_voronoi_edges()`](https://josiahparry.github.io/geoarrowrs/reference/ga_voronoi_edges.md).
- Added
  [`ga_orient()`](https://josiahparry.github.io/geoarrowrs/reference/ga_orient.md),
  [`ga_winding_order()`](https://josiahparry.github.io/geoarrowrs/reference/ga_winding_order.md),
  [`ga_is_ccw()`](https://josiahparry.github.io/geoarrowrs/reference/ga_is_ccw.md),
  and
  [`ga_is_cw()`](https://josiahparry.github.io/geoarrowrs/reference/ga_is_ccw.md).
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
  [`ga_relate()`](https://josiahparry.github.io/geoarrowrs/reference/ga_relate.md),
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
- Added
  [`read_shapefile()`](https://josiahparry.github.io/geoarrowrs/reference/read_shapefile.md),
  [`read_geojson()`](https://josiahparry.github.io/geoarrowrs/reference/read_geojson.md),
  and
  [`read_flatgeobuf()`](https://josiahparry.github.io/geoarrowrs/reference/read_flatgeobuf.md).
- Added
  [`ga_cast_geometry()`](https://josiahparry.github.io/geoarrowrs/reference/ga_cast_geometry.md)
  and
  [`ga_downcast_geometry()`](https://josiahparry.github.io/geoarrowrs/reference/ga_downcast_geometry.md)
  for converting between GeoArrow geometry types, and
  [`ga_explode()`](https://josiahparry.github.io/geoarrowrs/reference/ga_explode.md)/[`ga_flatten()`](https://josiahparry.github.io/geoarrowrs/reference/ga_flatten.md)
  for splitting multi-part geometries into their parts and collapsing
  them again.
- Added
  [`ga_triangulate_earcut()`](https://josiahparry.github.io/geoarrowrs/reference/ga_triangulate_earcut.md)
  and
  [`ga_triangulate_delaunay()`](https://josiahparry.github.io/geoarrowrs/reference/ga_triangulate_delaunay.md).
- Added
  [`ga_convex_hull()`](https://josiahparry.github.io/geoarrowrs/reference/ga_convex_hull.md),
  [`ga_concave_hull()`](https://josiahparry.github.io/geoarrowrs/reference/ga_concave_hull.md),
  [`ga_extremes()`](https://josiahparry.github.io/geoarrowrs/reference/ga_extremes.md),
  and the affine operations
  [`ga_translate()`](https://josiahparry.github.io/geoarrowrs/reference/ga_translate.md),
  [`ga_rotate_around_centroid()`](https://josiahparry.github.io/geoarrowrs/reference/ga_rotate_around_centroid.md),
  [`ga_rotate_around_center()`](https://josiahparry.github.io/geoarrowrs/reference/ga_rotate_around_center.md),
  [`ga_scale_xy()`](https://josiahparry.github.io/geoarrowrs/reference/ga_scale_xy.md),
  [`ga_skew()`](https://josiahparry.github.io/geoarrowrs/reference/ga_skew.md),
  and
  [`ga_skew_xy()`](https://josiahparry.github.io/geoarrowrs/reference/ga_skew_xy.md).
- Added
  [`ga_densify()`](https://josiahparry.github.io/geoarrowrs/reference/ga_densify.md),
  [`ga_interpolate_point()`](https://josiahparry.github.io/geoarrowrs/reference/ga_interpolate_point.md),
  [`ga_point_at_distance_between()`](https://josiahparry.github.io/geoarrowrs/reference/interpolate_between.md),
  [`ga_point_at_ratio_between()`](https://josiahparry.github.io/geoarrowrs/reference/interpolate_between.md),
  [`ga_points_along_line()`](https://josiahparry.github.io/geoarrowrs/reference/ga_points_along_line.md),
  [`ga_dest_euclidean()`](https://josiahparry.github.io/geoarrowrs/reference/destination.md),
  [`ga_dest_haversine()`](https://josiahparry.github.io/geoarrowrs/reference/destination.md),
  and
  [`ga_dest_geodesic()`](https://josiahparry.github.io/geoarrowrs/reference/destination.md).
- Added
  [`ga_to_degrees()`](https://josiahparry.github.io/geoarrowrs/reference/ga_to_degrees.md)
  and
  [`ga_to_radians()`](https://josiahparry.github.io/geoarrowrs/reference/ga_to_radians.md).
- Casting between geometry types now runs in parallel.
- The parallel paths use every core by default; set
  `options(geoarrowrs.thread_pool = n)` for a cap.
- Every function is vectorized and length preserving.
- Numeric arguments accept a plain R numeric vector as well as an Arrow
  array.
- Geometry arguments accept any GeoArrow array, not only a mixed
  `geometry` array.
