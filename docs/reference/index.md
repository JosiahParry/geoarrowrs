# Package index

## Read

Readers return a record batch stream. Everything else returns an Arrow
array.

- [`read_flatgeobuf()`](https://josiahparry.github.io/geoarrowrs/reference/read_flatgeobuf.md)
  : Read a FlatGeobuf file into a record batch stream
- [`read_geojson()`](https://josiahparry.github.io/geoarrowrs/reference/read_geojson.md)
  : Read a GeoJSON FeatureCollection into a record batch stream
- [`read_shapefile()`](https://josiahparry.github.io/geoarrowrs/reference/read_shapefile.md)
  : Read an ESRI Shapefile into a record batch stream

## Stay in Arrow

Register these functions with Arrow so they run on a Table or Dataset
without pulling geometry into R.

- [`register_geoarrow_udfs()`](https://josiahparry.github.io/geoarrowrs/reference/register_geoarrow_udfs.md)
  : Register geoarrowrs functions with Arrow

## Measure

Area, length, distance, and bearing. All return plain Arrow arrays.

- [`signed_area()`](https://josiahparry.github.io/geoarrowrs/reference/area.md)
  [`unsigned_area()`](https://josiahparry.github.io/geoarrowrs/reference/area.md)
  : Compute the signed and unsigned planar area of geometries
- [`signed_area_cd()`](https://josiahparry.github.io/geoarrowrs/reference/area_cd.md)
  [`unsigned_area_cd()`](https://josiahparry.github.io/geoarrowrs/reference/area_cd.md)
  : Compute the signed and unsigned area using the Chamberlain-Duquette
  algorithm
- [`signed_area_geodesic()`](https://josiahparry.github.io/geoarrowrs/reference/area_geodesic.md)
  [`unsigned_area_geodesic()`](https://josiahparry.github.io/geoarrowrs/reference/area_geodesic.md)
  [`perimeter_signed_geodesic()`](https://josiahparry.github.io/geoarrowrs/reference/area_geodesic.md)
  [`perimeter_unsigned_geodesic()`](https://josiahparry.github.io/geoarrowrs/reference/area_geodesic.md)
  : Compute the signed and unsigned geodesic area and perimeter of
  geometries
- [`length_euclidean()`](https://josiahparry.github.io/geoarrowrs/reference/length.md)
  [`length_haversine()`](https://josiahparry.github.io/geoarrowrs/reference/length.md)
  [`length_geodesic()`](https://josiahparry.github.io/geoarrowrs/reference/length.md)
  [`length_rhumb()`](https://josiahparry.github.io/geoarrowrs/reference/length.md)
  [`length_vincenty()`](https://josiahparry.github.io/geoarrowrs/reference/length.md)
  : Compute the length of linestrings
- [`dist_frechet_pairwise()`](https://josiahparry.github.io/geoarrowrs/reference/dist_frechet_pairwise.md)
  : Compute the pairwise Frechet distance between linestrings
- [`dist_hausdorff_pairwise()`](https://josiahparry.github.io/geoarrowrs/reference/dist_hausdorff_pairwise.md)
  : Compute the pairwise Hausdorff distance between geometries
- [`dist_euclidean_pairwise()`](https://josiahparry.github.io/geoarrowrs/reference/dist_pairwise.md)
  [`dist_haversine_pairwise()`](https://josiahparry.github.io/geoarrowrs/reference/dist_pairwise.md)
  [`dist_geodesic_pairwise()`](https://josiahparry.github.io/geoarrowrs/reference/dist_pairwise.md)
  [`dist_rhumb_pairwise()`](https://josiahparry.github.io/geoarrowrs/reference/dist_pairwise.md)
  : Compute pairwise distances between points
- [`dist_vincenty_pairwise()`](https://josiahparry.github.io/geoarrowrs/reference/dist_vincenty_pairwise.md)
  : Compute the pairwise Vincenty distance between points
- [`bearing_euclidean()`](https://josiahparry.github.io/geoarrowrs/reference/bearing.md)
  [`bearing_haversine()`](https://josiahparry.github.io/geoarrowrs/reference/bearing.md)
  [`bearing_geodesic()`](https://josiahparry.github.io/geoarrowrs/reference/bearing.md)
  [`bearing_rhumb()`](https://josiahparry.github.io/geoarrowrs/reference/bearing.md)
  : Compute the bearing between pairs of points
- [`dest_rhumb()`](https://josiahparry.github.io/geoarrowrs/reference/destination.md)
  [`dest_euclidean()`](https://josiahparry.github.io/geoarrowrs/reference/destination.md)
  [`dest_haversine()`](https://josiahparry.github.io/geoarrowrs/reference/destination.md)
  [`dest_geodesic()`](https://josiahparry.github.io/geoarrowrs/reference/destination.md)
  : Compute a destination point from an origin, bearing, and distance

## Index

Build an index once, query it many times. Results are candidates to
confirm with an exact predicate.

- [`RTree`](https://josiahparry.github.io/geoarrowrs/reference/RTree.md)
  : A packed Hilbert R-tree over the bounding boxes of a geometry array.
  A spatial index over a geometry array

## Relate

Topological predicates. Pairwise, with the second argument recycled.

- [`coordinate_position()`](https://josiahparry.github.io/geoarrowrs/reference/coordinate_position.md)
  : Locate a point relative to a geometry
- [`dimension()`](https://josiahparry.github.io/geoarrowrs/reference/dimension.md)
  [`boundary_dimension()`](https://josiahparry.github.io/geoarrowrs/reference/dimension.md)
  : Determine the topological dimension of geometries
- [`is_empty()`](https://josiahparry.github.io/geoarrowrs/reference/is_empty.md)
  : Test whether geometries are empty
- [`relate()`](https://josiahparry.github.io/geoarrowrs/reference/relate.md)
  : Compute the DE-9IM relationship between two geometry arrays
- [`contains()`](https://josiahparry.github.io/geoarrowrs/reference/topology.md)
  [`contains_properly()`](https://josiahparry.github.io/geoarrowrs/reference/topology.md)
  [`within()`](https://josiahparry.github.io/geoarrowrs/reference/topology.md)
  [`covers()`](https://josiahparry.github.io/geoarrowrs/reference/topology.md)
  [`covered_by()`](https://josiahparry.github.io/geoarrowrs/reference/topology.md)
  [`intersects()`](https://josiahparry.github.io/geoarrowrs/reference/topology.md)
  [`disjoint()`](https://josiahparry.github.io/geoarrowrs/reference/topology.md)
  [`touches()`](https://josiahparry.github.io/geoarrowrs/reference/topology.md)
  [`crosses()`](https://josiahparry.github.io/geoarrowrs/reference/topology.md)
  [`overlaps()`](https://josiahparry.github.io/geoarrowrs/reference/topology.md)
  [`equals_topo()`](https://josiahparry.github.io/geoarrowrs/reference/topology.md)
  : Test a topological relationship between two geometry arrays
- [`line_intersection()`](https://josiahparry.github.io/geoarrowrs/reference/line_intersection.md)
  : Intersect pairs of two point lines
- [`self_intersections()`](https://josiahparry.github.io/geoarrowrs/reference/self_intersections.md)
  : Find where a geometry crosses itself

## Inspect

Walk a geometry’s parts, and check it is well formed.

- [`coords()`](https://josiahparry.github.io/geoarrowrs/reference/coords.md)
  [`exterior_coords()`](https://josiahparry.github.io/geoarrowrs/reference/coords.md)
  : Collect a geometry's coordinates as points
- [`lines()`](https://josiahparry.github.io/geoarrowrs/reference/lines.md)
  : Split geometries into their line segments
- [`n_coords()`](https://josiahparry.github.io/geoarrowrs/reference/n_coords.md)
  : Count the coordinates in each geometry
- [`is_valid()`](https://josiahparry.github.io/geoarrowrs/reference/validation.md)
  [`validation_error()`](https://josiahparry.github.io/geoarrowrs/reference/validation.md)
  : Test whether geometries are well formed

## Cluster

Group the points within each row, or score them for outlyingness.

- [`dbscan()`](https://josiahparry.github.io/geoarrowrs/reference/dbscan.md)
  : Assign points to clusters by density
- [`kmeans()`](https://josiahparry.github.io/geoarrowrs/reference/kmeans.md)
  : Assign points to a fixed number of clusters
- [`outlier_scores()`](https://josiahparry.github.io/geoarrowrs/reference/outlier_scores.md)
  : Score how much each point looks like an outlier

## Combine

Set operations on polygons.

- [`boolean_intersection()`](https://josiahparry.github.io/geoarrowrs/reference/boolean_ops.md)
  [`boolean_union()`](https://josiahparry.github.io/geoarrowrs/reference/boolean_ops.md)
  [`boolean_difference()`](https://josiahparry.github.io/geoarrowrs/reference/boolean_ops.md)
  [`boolean_xor()`](https://josiahparry.github.io/geoarrowrs/reference/boolean_ops.md)
  : Combine two polygon arrays with a set operation
- [`unary_union()`](https://josiahparry.github.io/geoarrowrs/reference/unary_union.md)
  : Dissolve an entire array of polygons into one

## Reshape

Move, simplify, and convert. The geometry type is preserved.

- [`affine_transform()`](https://josiahparry.github.io/geoarrowrs/reference/affine_transform.md)
  : Apply an arbitrary affine transform
- [`rotate_around_center()`](https://josiahparry.github.io/geoarrowrs/reference/rotate_around_center.md)
  : Rotate geometries around the center of their bounding box
- [`rotate_around_centroid()`](https://josiahparry.github.io/geoarrowrs/reference/rotate_around_centroid.md)
  : Rotate geometries around their centroid
- [`scale_xy()`](https://josiahparry.github.io/geoarrowrs/reference/scale_xy.md)
  : Scale geometries independently in x and y about their centroid
- [`skew()`](https://josiahparry.github.io/geoarrowrs/reference/skew.md)
  : Skew geometries uniformly about their centroid
- [`skew_xy()`](https://josiahparry.github.io/geoarrowrs/reference/skew_xy.md)
  : Skew geometries independently in x and y about their centroid
- [`translate()`](https://josiahparry.github.io/geoarrowrs/reference/translate.md)
  : Translate geometries along the x and y axes
- [`simplify()`](https://josiahparry.github.io/geoarrowrs/reference/simplify.md)
  : Simplify geometries using the Ramer-Douglas-Peucker algorithm
- [`simplify_idx()`](https://josiahparry.github.io/geoarrowrs/reference/simplify_idx.md)
  [`simplify_vw_idx()`](https://josiahparry.github.io/geoarrowrs/reference/simplify_idx.md)
  : Find which coordinates simplification would keep
- [`simplify_vw()`](https://josiahparry.github.io/geoarrowrs/reference/simplify_vw.md)
  [`simplify_vw_preserve()`](https://josiahparry.github.io/geoarrowrs/reference/simplify_vw.md)
  : Simplify geometries using the Visvalingam-Whyatt algorithm
- [`to_degrees()`](https://josiahparry.github.io/geoarrowrs/reference/to_degrees.md)
  : Convert coordinates from radians to degrees
- [`to_radians()`](https://josiahparry.github.io/geoarrowrs/reference/to_radians.md)
  : Convert coordinates from degrees to radians
- [`buffer()`](https://josiahparry.github.io/geoarrowrs/reference/buffer.md)
  : Buffer geometries by a given distance
- [`centroid()`](https://josiahparry.github.io/geoarrowrs/reference/centroid.md)
  : Compute the centroid of geometries
- [`chaikin_smoothing()`](https://josiahparry.github.io/geoarrowrs/reference/chaikin_smoothing.md)
  : Smooth geometries using the Chaikin algorithm
- [`line_segmentize()`](https://josiahparry.github.io/geoarrowrs/reference/line_segmentize.md)
  [`line_segmentize_haversine()`](https://josiahparry.github.io/geoarrowrs/reference/line_segmentize.md)
  : Split linestrings into a given number of equal-length segments
- [`remove_repeated_points()`](https://josiahparry.github.io/geoarrowrs/reference/remove_repeated_points.md)
  : Remove repeated consecutive points from geometries

## Derive

Build new geometry from existing geometry.

- [`bounding_rect()`](https://josiahparry.github.io/geoarrowrs/reference/bounding_rect.md)
  : Compute the axis-aligned bounding rectangle of geometries
- [`concave_hull()`](https://josiahparry.github.io/geoarrowrs/reference/concave_hull.md)
  : Compute the concave hull of geometries
- [`convex_hull()`](https://josiahparry.github.io/geoarrowrs/reference/convex_hull.md)
  : Compute the convex hull of geometries
- [`extremes()`](https://josiahparry.github.io/geoarrowrs/reference/extremes.md)
  : Compute the extreme coordinates of geometries
- [`minimum_rotated_rect()`](https://josiahparry.github.io/geoarrowrs/reference/minimum_rotated_rect.md)
  : Compute the minimum rotated bounding rectangle of geometries
- [`triangulate_delaunay()`](https://josiahparry.github.io/geoarrowrs/reference/triangulate_delaunay.md)
  : Triangulate geometries with a Delaunay triangulation
- [`triangulate_earcut()`](https://josiahparry.github.io/geoarrowrs/reference/triangulate_earcut.md)
  : Triangulate polygons with the earcut algorithm
- [`voronoi_cells()`](https://josiahparry.github.io/geoarrowrs/reference/voronoi_cells.md)
  : Compute Voronoi cells from the vertices of geometries
- [`voronoi_edges()`](https://josiahparry.github.io/geoarrowrs/reference/voronoi_edges.md)
  : Compute Voronoi edges from the vertices of geometries
- [`closest_point()`](https://josiahparry.github.io/geoarrowrs/reference/closest_point.md)
  [`closest_point_haversine()`](https://josiahparry.github.io/geoarrowrs/reference/closest_point.md)
  : Find the point on a geometry closest to another point
- [`interior_point()`](https://josiahparry.github.io/geoarrowrs/reference/interior_point.md)
  : Compute a representative point inside a geometry
- [`is_convex()`](https://josiahparry.github.io/geoarrowrs/reference/is_convex.md)
  : Test whether a ring is convex
- [`line_locate_point()`](https://josiahparry.github.io/geoarrowrs/reference/line_locate_point.md)
  : Locate a point along a line as a fraction of its length
- [`is_ccw()`](https://josiahparry.github.io/geoarrowrs/reference/is_ccw.md)
  [`is_cw()`](https://josiahparry.github.io/geoarrowrs/reference/is_ccw.md)
  : Test the winding order of a ring
- [`orient()`](https://josiahparry.github.io/geoarrowrs/reference/orient.md)
  : Apply a winding direction to polygon rings
- [`winding_order()`](https://josiahparry.github.io/geoarrowrs/reference/winding_order.md)
  : Determine the winding order of a ring

## Interpolate

Place points along lines and add vertices.

- [`point_at_distance_between()`](https://josiahparry.github.io/geoarrowrs/reference/interpolate_between.md)
  [`point_at_ratio_between()`](https://josiahparry.github.io/geoarrowrs/reference/interpolate_between.md)
  : Interpolate a point at a given distance between two points
- [`interpolate_point()`](https://josiahparry.github.io/geoarrowrs/reference/interpolate_point.md)
  : Interpolate a point along a linestring
- [`points_along_line()`](https://josiahparry.github.io/geoarrowrs/reference/points_along_line.md)
  : Generate points at regular intervals along the line between two
  points
- [`densify()`](https://josiahparry.github.io/geoarrowrs/reference/densify.md)
  : Add intermediate points to geometries so no segment exceeds a
  maximum length

## Cast

Convert between geometry types and split multi-part geometries.

- [`cast_geometry()`](https://josiahparry.github.io/geoarrowrs/reference/cast_geometry.md)
  : Cast geometries to another GeoArrow geometry type
- [`downcast_geometry()`](https://josiahparry.github.io/geoarrowrs/reference/downcast_geometry.md)
  : Cast geometries to the narrowest type that fits them
- [`explode()`](https://josiahparry.github.io/geoarrowrs/reference/explode.md)
  : Split multi-part geometries into their parts
- [`flatten()`](https://josiahparry.github.io/geoarrowrs/reference/flatten.md)
  : Collapse a list of geometries into a single array
