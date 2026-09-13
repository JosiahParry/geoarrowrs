# geoarrowrs (development version)

* A `geoarrow_vctr` is now an sdf geometry backend, implementing sdf's generics so `sdf_filter()` and `sdf_join()` run on Arrow.
* Added `mask_sf()`, which attaches sf's names and argument names for the GeoArrow representation, alongside S3 methods on `geoarrow_vctr` for the ones sf makes generic.
* `ga_collect_agg()` takes `by`, an unsorted key of one label per row, and groups the rows itself instead of requiring an `arrange()` first.
* Added `ga_set_thread_pool()`, which sets the thread cap used by the parallel paths.
* `ga_sparse_knn()` and `ga_sparse_dwithin()` take `metric`, one of `"euclidean"`, `"haversine"`, `"geodesic"`, or `"rhumb"`.
* Added `ga_filter()`, which keeps the rows of one data frame that relate to any row of another.
* Added `ga_sparse_pairs()`, which expands a sparse predicate's list result into one row per pair.
* Added `ga_cross_distance()`, which measures every row of `x` against every row of `y`, returning one list of distances per row.
* Added `ga_sparse_knn()` and `ga_knn_join()`, a nearest neighbour search and the join over it.
* Materialising geometries now runs in parallel throughout the package.
* `ga_collect_agg()` takes `sizes`, the number of rows in each group, and returns one collection per group instead of one for the whole array.
* Reading geometries splits into more, smaller rayon tasks, sized against the thread pool.
* Added `ga_sparse_dwithin()`, which finds the rows of `y` within a distance of each row of `x`.
* A point on either side of a sparse predicate is now answered from where the point lies rather than from a DE-9IM matrix.
* The topological predicates node each geometry once rather than once per comparison.
* `RTree` takes arrays everywhere, matching `KDTree`.
* Added `ga_collect_agg()`, which gathers a whole array into one geometry collection of length 1.
* `ga_dist_euclidean_pairwise()` and `ga_dist_hausdorff_pairwise()` measure between geometries of any type, not just points.
* Added `ga_make_line()`, which joins each point of one array to the matching point of another.
* Added `ga_x()` and `ga_y()`, which read the coordinates of a point array as doubles.
* The distance and bearing functions recycle a length 1 `dest`.
* Added `ga_join()`, which attaches the columns of one data frame to each row of another it relates to spatially.
* Added the sparse predicates `ga_sparse_intersects()`, `ga_sparse_contains()`, `ga_sparse_contains_properly()`, `ga_sparse_within()`, `ga_sparse_covers()`, `ga_sparse_covered_by()`, `ga_sparse_touches()`, `ga_sparse_crosses()`, `ga_sparse_overlaps()`, and `ga_sparse_equals_topo()`.
* Added `KDTree`, a k-d tree over a point array.
* Added `RTree`, a packed Hilbert R-tree over each geometry's bounding box.
* Added `ga_envelope()`, which returns the axis aligned bounding box of each geometry.
* Added `ga_dbscan()`, `ga_kmeans()`, `ga_outlier_scores()`, `ga_simplify_idx()`, and `ga_simplify_vw_idx()`.
* Added `ga_as_point()`, `ga_as_linestring()`, `ga_as_polygon()`, `ga_as_multipoint()`, `ga_as_multilinestring()`, and `ga_as_multipolygon()`.
* Added `ga_from_wkb()`, which reads a bare `binary` WKB column into the narrowest GeoArrow type that fits it.
* Added `ga_xy()`, which pairs two numeric vectors or float64 arrays into a GeoArrow point array.
* Added `register_geoarrow_udfs()`, which registers geoarrowrs functions as Arrow scalar kernels.
* Loading the package registers the Arrow kernels for CRS-less data automatically.
* Registered Arrow kernels accept a bare `binary` or `large_binary` WKB column.
* Added `ga_line_intersection()` and `ga_self_intersections()`.
* Added `ga_affine_transform()`.
* Added `ga_coords()`, `ga_exterior_coords()`, `ga_n_coords()`, `ga_lines()`, `ga_is_valid()`, and `ga_validation_error()`.
* Added `ga_voronoi_cells()` and `ga_voronoi_edges()`.
* Added `ga_orient()`, `ga_winding_order()`, `ga_is_ccw()`, and `ga_is_cw()`.
* Added the topological predicates `ga_contains()`, `ga_contains_properly()`, `ga_within()`, `ga_covers()`, `ga_covered_by()`, `ga_intersects()`, `ga_disjoint()`, `ga_touches()`, `ga_crosses()`, `ga_overlaps()`, and `ga_equals_topo()`, plus `ga_relate()`, `ga_dimension()`, `ga_boundary_dimension()`, `ga_is_empty()`, and `ga_coordinate_position()`.
* Added `ga_boolean_intersection()`, `ga_boolean_union()`, `ga_boolean_difference()`, `ga_boolean_xor()`, and `ga_unary_union()`.
* Added `ga_closest_point()`, `ga_closest_point_haversine()`, `ga_interior_point()`, `ga_is_convex()`, and `ga_line_locate_point()`.
* Added `read_shapefile()`, `read_geojson()`, and `read_flatgeobuf()`.
* Added `ga_cast_geometry()` and `ga_downcast_geometry()` for converting between GeoArrow geometry types, and `ga_explode()`/`ga_flatten()` for splitting multi-part geometries into their parts and collapsing them again.
* Added `ga_triangulate_earcut()` and `ga_triangulate_delaunay()`.
* Added `ga_convex_hull()`, `ga_concave_hull()`, `ga_extremes()`, and the affine operations `ga_translate()`, `ga_rotate_around_centroid()`, `ga_rotate_around_center()`, `ga_scale_xy()`, `ga_skew()`, and `ga_skew_xy()`.
* Added `ga_densify()`, `ga_interpolate_point()`, `ga_point_at_distance_between()`, `ga_point_at_ratio_between()`, `ga_points_along_line()`, `ga_dest_euclidean()`, `ga_dest_haversine()`, and `ga_dest_geodesic()`.
* Added `ga_to_degrees()` and `ga_to_radians()`.
* Casting between geometry types now runs in parallel.
* The parallel paths use every core by default; set `options(geoarrowrs.thread_pool = n)` for a cap.
* Every function is vectorized and length preserving.
* Numeric arguments accept a plain R numeric vector as well as an Arrow array.
* Geometry arguments accept any GeoArrow array, not only a mixed `geometry` array.
