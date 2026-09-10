# Changelog

## geoarrowrs (development version)

- Added
  [`voronoi_cells()`](https://josiahparry.github.io/geoarrowrs/reference/voronoi_cells.md)
  and
  [`voronoi_edges()`](https://josiahparry.github.io/geoarrowrs/reference/voronoi_edges.md),
  returning one multipolygon of cells or multilinestring of edges per
  input geometry. Both take a `clip` mode and an optional `boundary`
  polygon to cut the diagram to a study area.
- Added
  [`orient()`](https://josiahparry.github.io/geoarrowrs/reference/orient.md),
  [`winding_order()`](https://josiahparry.github.io/geoarrowrs/reference/winding_order.md),
  [`is_ccw()`](https://josiahparry.github.io/geoarrowrs/reference/is_ccw.md),
  and
  [`is_cw()`](https://josiahparry.github.io/geoarrowrs/reference/is_ccw.md).
  A multi part geometry reports a winding only when all of its parts
  agree.
- Added
  [`register_geoarrow_udfs()`](https://josiahparry.github.io/geoarrowrs/reference/register_geoarrow_udfs.md),
  which registers geoarrowrs functions as Arrow scalar kernels so they
  run inside `dplyr` verbs on a `Table` or `Dataset` rather than pulling
  the geometry into R.
- Added the topological predicates
  [`contains()`](https://josiahparry.github.io/geoarrowrs/reference/topology.md),
  [`contains_properly()`](https://josiahparry.github.io/geoarrowrs/reference/topology.md),
  [`within()`](https://josiahparry.github.io/geoarrowrs/reference/topology.md),
  [`covers()`](https://josiahparry.github.io/geoarrowrs/reference/topology.md),
  [`covered_by()`](https://josiahparry.github.io/geoarrowrs/reference/topology.md),
  [`intersects()`](https://josiahparry.github.io/geoarrowrs/reference/topology.md),
  [`disjoint()`](https://josiahparry.github.io/geoarrowrs/reference/topology.md),
  [`touches()`](https://josiahparry.github.io/geoarrowrs/reference/topology.md),
  [`crosses()`](https://josiahparry.github.io/geoarrowrs/reference/topology.md),
  [`overlaps()`](https://josiahparry.github.io/geoarrowrs/reference/topology.md),
  and
  [`equals_topo()`](https://josiahparry.github.io/geoarrowrs/reference/topology.md),
  plus
  [`relate()`](https://josiahparry.github.io/geoarrowrs/reference/relate.md)
  for the DE-9IM string,
  [`dimension()`](https://josiahparry.github.io/geoarrowrs/reference/dimension.md),
  [`boundary_dimension()`](https://josiahparry.github.io/geoarrowrs/reference/dimension.md),
  [`is_empty()`](https://josiahparry.github.io/geoarrowrs/reference/is_empty.md),
  and
  [`coordinate_position()`](https://josiahparry.github.io/geoarrowrs/reference/coordinate_position.md).
- Added
  [`boolean_intersection()`](https://josiahparry.github.io/geoarrowrs/reference/boolean_ops.md),
  [`boolean_union()`](https://josiahparry.github.io/geoarrowrs/reference/boolean_ops.md),
  [`boolean_difference()`](https://josiahparry.github.io/geoarrowrs/reference/boolean_ops.md),
  [`boolean_xor()`](https://josiahparry.github.io/geoarrowrs/reference/boolean_ops.md),
  and
  [`unary_union()`](https://josiahparry.github.io/geoarrowrs/reference/unary_union.md).
- Added
  [`closest_point()`](https://josiahparry.github.io/geoarrowrs/reference/closest_point.md),
  [`closest_point_haversine()`](https://josiahparry.github.io/geoarrowrs/reference/closest_point.md),
  [`interior_point()`](https://josiahparry.github.io/geoarrowrs/reference/interior_point.md),
  [`is_convex()`](https://josiahparry.github.io/geoarrowrs/reference/is_convex.md),
  and
  [`line_locate_point()`](https://josiahparry.github.io/geoarrowrs/reference/line_locate_point.md).
  [`interior_point()`](https://josiahparry.github.io/geoarrowrs/reference/interior_point.md)
  always lands on the geometry, unlike
  [`centroid()`](https://josiahparry.github.io/geoarrowrs/reference/centroid.md).
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
  [`cast_geometry()`](https://josiahparry.github.io/geoarrowrs/reference/cast_geometry.md)
  and
  [`downcast_geometry()`](https://josiahparry.github.io/geoarrowrs/reference/downcast_geometry.md)
  for converting between GeoArrow geometry types, and
  [`explode()`](https://josiahparry.github.io/geoarrowrs/reference/explode.md)/[`flatten()`](https://josiahparry.github.io/geoarrowrs/reference/flatten.md)
  for splitting multi-part geometries into their parts and collapsing
  them again.
  [`explode()`](https://josiahparry.github.io/geoarrowrs/reference/explode.md)
  preserves length, returning one array of parts per row.
- Added
  [`triangulate_earcut()`](https://josiahparry.github.io/geoarrowrs/reference/triangulate_earcut.md)
  and
  [`triangulate_delaunay()`](https://josiahparry.github.io/geoarrowrs/reference/triangulate_delaunay.md),
  returning one multipolygon of triangles per input geometry.
- Added
  [`convex_hull()`](https://josiahparry.github.io/geoarrowrs/reference/convex_hull.md),
  [`concave_hull()`](https://josiahparry.github.io/geoarrowrs/reference/concave_hull.md),
  and
  [`extremes()`](https://josiahparry.github.io/geoarrowrs/reference/extremes.md),
  plus the affine operations
  [`translate()`](https://josiahparry.github.io/geoarrowrs/reference/translate.md),
  [`rotate_around_centroid()`](https://josiahparry.github.io/geoarrowrs/reference/rotate_around_centroid.md),
  [`rotate_around_center()`](https://josiahparry.github.io/geoarrowrs/reference/rotate_around_center.md),
  [`scale_xy()`](https://josiahparry.github.io/geoarrowrs/reference/scale_xy.md),
  [`skew()`](https://josiahparry.github.io/geoarrowrs/reference/skew.md),
  and
  [`skew_xy()`](https://josiahparry.github.io/geoarrowrs/reference/skew_xy.md).
- Added
  [`to_degrees()`](https://josiahparry.github.io/geoarrowrs/reference/to_degrees.md)
  and
  [`to_radians()`](https://josiahparry.github.io/geoarrowrs/reference/to_radians.md).
- Numeric arguments now accept a plain R numeric vector as well as an
  Arrow array, so `simplify(x, 0.01)` works without wrapping the value.
- Geometry arguments now accept any GeoArrow array rather than only a
  mixed `geometry` array.
  [`centroid()`](https://josiahparry.github.io/geoarrowrs/reference/centroid.md),
  [`signed_area()`](https://josiahparry.github.io/geoarrowrs/reference/area.md),
  [`buffer()`](https://josiahparry.github.io/geoarrowrs/reference/buffer.md)
  and ten others previously errored on the multipolygon arrays the
  readers produce.
- Empty points no longer crash. Every geo-backed function panicked on an
  array containing one, because `to_geometry()` cannot represent an
  empty point.
- [`densify()`](https://josiahparry.github.io/geoarrowrs/reference/densify.md),
  [`interpolate_point()`](https://josiahparry.github.io/geoarrowrs/reference/interpolate_point.md),
  [`point_at_distance_between()`](https://josiahparry.github.io/geoarrowrs/reference/interpolate_between.md),
  [`point_at_ratio_between()`](https://josiahparry.github.io/geoarrowrs/reference/interpolate_between.md),
  [`points_along_line()`](https://josiahparry.github.io/geoarrowrs/reference/points_along_line.md),
  [`dest_euclidean()`](https://josiahparry.github.io/geoarrowrs/reference/destination.md),
  [`dest_haversine()`](https://josiahparry.github.io/geoarrowrs/reference/destination.md),
  and
  [`dest_geodesic()`](https://josiahparry.github.io/geoarrowrs/reference/destination.md)
  are now exported. They were written but never registered.
- [`point_at_ratio_between()`](https://josiahparry.github.io/geoarrowrs/reference/interpolate_between.md)
  returned `NULL` instead of its result, and the `interpolate_point`
  functions ignored recycling despite documenting it.
