# geoarrowrs (development version)

* Added `line_intersection()` and `self_intersections()`, the latter using the Bentley-Ottmann sweep to locate exactly where a geometry crosses itself.
* Added `affine_transform()`, the general form behind the other affine ops, taking six recyclable coefficients so a different transform can apply to every row.
* Added `dbscan()`, `kmeans()`, and `outlier_scores()` for clustering the points within each row, plus `simplify_idx()` and `simplify_vw_idx()`, which return the coordinate positions simplification keeps rather than the simplified geometry.
* Added `coords()`, `exterior_coords()`, `n_coords()`, and `lines()` for walking a geometry's vertices and segments, plus `is_valid()` and `validation_error()`.
* Added an `RTree` spatial index built on the `geo-index` crate, with `$search()` for bounding box queries and `$neighbors()` for nearest rows. Queries return candidates to confirm with an exact predicate.
* Added `voronoi_cells()` and `voronoi_edges()`, returning one multipolygon of cells or multilinestring of edges per input geometry. Both take a `clip` mode and an optional `boundary` polygon to cut the diagram to a study area.
* Added `orient()`, `winding_order()`, `is_ccw()`, and `is_cw()`. A multi part geometry reports a winding only when all of its parts agree.
* Added `register_geoarrow_udfs()`, which registers geoarrowrs functions as Arrow scalar kernels so they run inside `dplyr` verbs on a `Table` or `Dataset` rather than pulling the geometry into R. It registers for a CRS rather than for a table: one call covers every GeoArrow geometry type, and functions taking numeric or option arguments, such as `simplify()`, `densify()`, and `buffer()`, are registered alongside the geometry-only ones. 101 functions in total.
* Added the topological predicates `contains()`, `contains_properly()`, `within()`, `covers()`, `covered_by()`, `intersects()`, `disjoint()`, `touches()`, `crosses()`, `overlaps()`, and `equals_topo()`, plus `relate()` for the DE-9IM string, `dimension()`, `boundary_dimension()`, `is_empty()`, and `coordinate_position()`.
* Added `boolean_intersection()`, `boolean_union()`, `boolean_difference()`, `boolean_xor()`, and `unary_union()`.
* Added `closest_point()`, `closest_point_haversine()`, `interior_point()`, `is_convex()`, and `line_locate_point()`. `interior_point()` always lands on the geometry, unlike `centroid()`.
* Added `read_shapefile()`, `read_geojson()`, and `read_flatgeobuf()`, each returning a record batch stream of the attribute columns followed by a `geometry` column. `read_flatgeobuf()` streams and takes an optional `bbox` that uses the file's spatial index.
* Added `cast_geometry()` and `downcast_geometry()` for converting between GeoArrow geometry types, and `explode()`/`flatten()` for splitting multi-part geometries into their parts and collapsing them again. `explode()` preserves length, returning one array of parts per row.
* Added `triangulate_earcut()` and `triangulate_delaunay()`, returning one multipolygon of triangles per input geometry.
* Added `convex_hull()`, `concave_hull()`, and `extremes()`, plus the affine operations `translate()`, `rotate_around_centroid()`, `rotate_around_center()`, `scale_xy()`, `skew()`, and `skew_xy()`.
* Added `to_degrees()` and `to_radians()`.
* Numeric arguments now accept a plain R numeric vector as well as an Arrow array, so `simplify(x, 0.01)` works without wrapping the value.
* Geometry arguments now accept any GeoArrow array rather than only a mixed `geometry` array. `centroid()`, `signed_area()`, `buffer()` and ten others previously errored on the multipolygon arrays the readers produce.
* Empty points no longer crash. Every geo-backed function panicked on an array containing one, because `to_geometry()` cannot represent an empty point.
* `densify()`, `interpolate_point()`, `point_at_distance_between()`, `point_at_ratio_between()`, `points_along_line()`, `dest_euclidean()`, `dest_haversine()`, and `dest_geodesic()` are now exported. They were written but never registered.
* `point_at_ratio_between()` returned `NULL` instead of its result, and the `interpolate_point` functions ignored recycling despite documenting it.
