# geoarrowrs (development version)

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
