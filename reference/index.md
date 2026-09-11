# Package index

## Read

- [`read_flatgeobuf()`](https://josiahparry.github.io/geoarrowrs/reference/read_flatgeobuf.md)
  : Read a FlatGeobuf file into a record batch stream
- [`read_geojson()`](https://josiahparry.github.io/geoarrowrs/reference/read_geojson.md)
  : Read a GeoJSON FeatureCollection
- [`read_shapefile()`](https://josiahparry.github.io/geoarrowrs/reference/read_shapefile.md)
  : Read an ESRI Shapefile into a record batch stream

## Construct

- [`ga_xy()`](https://josiahparry.github.io/geoarrowrs/reference/ga_xy.md)
  : Build a point array from x and y coordinates

## Area

- [`ga_signed_area()`](https://josiahparry.github.io/geoarrowrs/reference/area.md)
  [`ga_unsigned_area()`](https://josiahparry.github.io/geoarrowrs/reference/area.md)
  : Signed and unsigned planar area
- [`ga_signed_area_cd()`](https://josiahparry.github.io/geoarrowrs/reference/area_cd.md)
  [`ga_unsigned_area_cd()`](https://josiahparry.github.io/geoarrowrs/reference/area_cd.md)
  : Chamberlain-Duquette spherical area
- [`ga_signed_area_geodesic()`](https://josiahparry.github.io/geoarrowrs/reference/area_geodesic.md)
  [`ga_unsigned_area_geodesic()`](https://josiahparry.github.io/geoarrowrs/reference/area_geodesic.md)
  [`ga_perimeter_geodesic()`](https://josiahparry.github.io/geoarrowrs/reference/area_geodesic.md)
  : Geodesic area and perimeter

## Distance

- [`ga_dist_euclidean_pairwise()`](https://josiahparry.github.io/geoarrowrs/reference/dist_pairwise.md)
  [`ga_dist_haversine_pairwise()`](https://josiahparry.github.io/geoarrowrs/reference/dist_pairwise.md)
  [`ga_dist_geodesic_pairwise()`](https://josiahparry.github.io/geoarrowrs/reference/dist_pairwise.md)
  [`ga_dist_rhumb_pairwise()`](https://josiahparry.github.io/geoarrowrs/reference/dist_pairwise.md)
  : Compute pairwise distances between points
- [`ga_dist_frechet_pairwise()`](https://josiahparry.github.io/geoarrowrs/reference/ga_dist_frechet_pairwise.md)
  : Pairwise Frechet distance
- [`ga_dist_hausdorff_pairwise()`](https://josiahparry.github.io/geoarrowrs/reference/ga_dist_hausdorff_pairwise.md)
  : Pairwise Hausdorff distance
- [`ga_dist_vincenty_pairwise()`](https://josiahparry.github.io/geoarrowrs/reference/ga_dist_vincenty_pairwise.md)
  : Pairwise Vincenty distance

## Measure

- [`ga_length_euclidean()`](https://josiahparry.github.io/geoarrowrs/reference/length.md)
  [`ga_length_haversine()`](https://josiahparry.github.io/geoarrowrs/reference/length.md)
  [`ga_length_geodesic()`](https://josiahparry.github.io/geoarrowrs/reference/length.md)
  [`ga_length_rhumb()`](https://josiahparry.github.io/geoarrowrs/reference/length.md)
  [`ga_length_vincenty()`](https://josiahparry.github.io/geoarrowrs/reference/length.md)
  : Compute the length of linestrings
- [`ga_bearing_euclidean()`](https://josiahparry.github.io/geoarrowrs/reference/bearing.md)
  [`ga_bearing_haversine()`](https://josiahparry.github.io/geoarrowrs/reference/bearing.md)
  [`ga_bearing_geodesic()`](https://josiahparry.github.io/geoarrowrs/reference/bearing.md)
  [`ga_bearing_rhumb()`](https://josiahparry.github.io/geoarrowrs/reference/bearing.md)
  : Compute the bearing between pairs of points
- [`ga_dest_rhumb()`](https://josiahparry.github.io/geoarrowrs/reference/destination.md)
  [`ga_dest_euclidean()`](https://josiahparry.github.io/geoarrowrs/reference/destination.md)
  [`ga_dest_haversine()`](https://josiahparry.github.io/geoarrowrs/reference/destination.md)
  [`ga_dest_geodesic()`](https://josiahparry.github.io/geoarrowrs/reference/destination.md)
  : Destination from origin, bearing, distance

## Index

- [`KDTree`](https://josiahparry.github.io/geoarrowrs/reference/KDTree.md)
  : A k-d tree over a point array
- [`RTree`](https://josiahparry.github.io/geoarrowrs/reference/RTree.md)
  : A spatial index over a geometry array
- [`ga_envelope()`](https://josiahparry.github.io/geoarrowrs/reference/ga_envelope.md)
  : Compute the bounding box of geometries

## Relate

- [`ga_coordinate_position()`](https://josiahparry.github.io/geoarrowrs/reference/ga_coordinate_position.md)
  : Locate a point relative to a geometry

- [`ga_dimension()`](https://josiahparry.github.io/geoarrowrs/reference/ga_dimension.md)
  [`ga_boundary_dimension()`](https://josiahparry.github.io/geoarrowrs/reference/ga_dimension.md)
  : Determine the topological dimension of geometries

- [`ga_is_empty()`](https://josiahparry.github.io/geoarrowrs/reference/ga_is_empty.md)
  : Test whether geometries are empty

- [`ga_join()`](https://josiahparry.github.io/geoarrowrs/reference/ga_join.md)
  : Join two data frames on a spatial relationship

- [`ga_relate()`](https://josiahparry.github.io/geoarrowrs/reference/ga_relate.md)
  : DE-9IM relationship between geometries

- [`ga_sparse_intersects()`](https://josiahparry.github.io/geoarrowrs/reference/sparse.md)
  [`ga_sparse_contains()`](https://josiahparry.github.io/geoarrowrs/reference/sparse.md)
  [`ga_sparse_contains_properly()`](https://josiahparry.github.io/geoarrowrs/reference/sparse.md)
  [`ga_sparse_within()`](https://josiahparry.github.io/geoarrowrs/reference/sparse.md)
  [`ga_sparse_covers()`](https://josiahparry.github.io/geoarrowrs/reference/sparse.md)
  [`ga_sparse_covered_by()`](https://josiahparry.github.io/geoarrowrs/reference/sparse.md)
  [`ga_sparse_touches()`](https://josiahparry.github.io/geoarrowrs/reference/sparse.md)
  [`ga_sparse_crosses()`](https://josiahparry.github.io/geoarrowrs/reference/sparse.md)
  [`ga_sparse_overlaps()`](https://josiahparry.github.io/geoarrowrs/reference/sparse.md)
  [`ga_sparse_equals_topo()`](https://josiahparry.github.io/geoarrowrs/reference/sparse.md)
  :

  Find which rows of `y` relate to each row of `x`

- [`ga_contains()`](https://josiahparry.github.io/geoarrowrs/reference/topology.md)
  [`ga_contains_properly()`](https://josiahparry.github.io/geoarrowrs/reference/topology.md)
  [`ga_within()`](https://josiahparry.github.io/geoarrowrs/reference/topology.md)
  [`ga_covers()`](https://josiahparry.github.io/geoarrowrs/reference/topology.md)
  [`ga_covered_by()`](https://josiahparry.github.io/geoarrowrs/reference/topology.md)
  [`ga_intersects()`](https://josiahparry.github.io/geoarrowrs/reference/topology.md)
  [`ga_disjoint()`](https://josiahparry.github.io/geoarrowrs/reference/topology.md)
  [`ga_touches()`](https://josiahparry.github.io/geoarrowrs/reference/topology.md)
  [`ga_crosses()`](https://josiahparry.github.io/geoarrowrs/reference/topology.md)
  [`ga_overlaps()`](https://josiahparry.github.io/geoarrowrs/reference/topology.md)
  [`ga_equals_topo()`](https://josiahparry.github.io/geoarrowrs/reference/topology.md)
  : Test a topological relationship

- [`ga_line_intersection()`](https://josiahparry.github.io/geoarrowrs/reference/ga_line_intersection.md)
  : Intersect pairs of two point lines

- [`ga_self_intersections()`](https://josiahparry.github.io/geoarrowrs/reference/ga_self_intersections.md)
  : Find where a geometry crosses itself

## Locate

- [`ga_closest_point()`](https://josiahparry.github.io/geoarrowrs/reference/ga_closest_point.md)
  [`ga_closest_point_haversine()`](https://josiahparry.github.io/geoarrowrs/reference/ga_closest_point.md)
  : Closest point on a geometry
- [`ga_interior_point()`](https://josiahparry.github.io/geoarrowrs/reference/ga_interior_point.md)
  : Compute a representative point inside a geometry
- [`ga_is_convex()`](https://josiahparry.github.io/geoarrowrs/reference/ga_is_convex.md)
  : Test whether a ring is convex
- [`ga_line_locate_point()`](https://josiahparry.github.io/geoarrowrs/reference/ga_line_locate_point.md)
  : Locate a point along a line

## Hulls and bounds

- [`ga_bounding_rect()`](https://josiahparry.github.io/geoarrowrs/reference/ga_bounding_rect.md)
  : Axis-aligned bounding rectangle
- [`ga_concave_hull()`](https://josiahparry.github.io/geoarrowrs/reference/ga_concave_hull.md)
  : Compute the concave hull of geometries
- [`ga_convex_hull()`](https://josiahparry.github.io/geoarrowrs/reference/ga_convex_hull.md)
  : Compute the convex hull of geometries
- [`ga_extremes()`](https://josiahparry.github.io/geoarrowrs/reference/ga_extremes.md)
  : Compute the extreme coordinates of geometries
- [`ga_minimum_rotated_rect()`](https://josiahparry.github.io/geoarrowrs/reference/ga_minimum_rotated_rect.md)
  : Minimum rotated bounding rectangle

## Tessellate

- [`ga_triangulate_delaunay()`](https://josiahparry.github.io/geoarrowrs/reference/ga_triangulate_delaunay.md)
  : Delaunay triangulation
- [`ga_triangulate_earcut()`](https://josiahparry.github.io/geoarrowrs/reference/ga_triangulate_earcut.md)
  : Triangulate polygons with the earcut algorithm
- [`ga_voronoi_cells()`](https://josiahparry.github.io/geoarrowrs/reference/ga_voronoi_cells.md)
  : Voronoi cells from geometry vertices
- [`ga_voronoi_edges()`](https://josiahparry.github.io/geoarrowrs/reference/ga_voronoi_edges.md)
  : Voronoi edges from geometry vertices

## Cluster

- [`ga_dbscan()`](https://josiahparry.github.io/geoarrowrs/reference/ga_dbscan.md)
  : Assign points to clusters by density
- [`ga_kmeans()`](https://josiahparry.github.io/geoarrowrs/reference/ga_kmeans.md)
  : Assign points to a fixed number of clusters
- [`ga_outlier_scores()`](https://josiahparry.github.io/geoarrowrs/reference/ga_outlier_scores.md)
  : Score how much each point looks like an outlier

## Combine

- [`ga_boolean_intersection()`](https://josiahparry.github.io/geoarrowrs/reference/boolean_ops.md)
  [`ga_boolean_union()`](https://josiahparry.github.io/geoarrowrs/reference/boolean_ops.md)
  [`ga_boolean_difference()`](https://josiahparry.github.io/geoarrowrs/reference/boolean_ops.md)
  [`ga_boolean_xor()`](https://josiahparry.github.io/geoarrowrs/reference/boolean_ops.md)
  : Combine two polygon arrays with a set operation
- [`ga_unary_union()`](https://josiahparry.github.io/geoarrowrs/reference/ga_unary_union.md)
  : Dissolve an entire array of polygons into one

## Transform

- [`ga_affine_transform()`](https://josiahparry.github.io/geoarrowrs/reference/ga_affine_transform.md)
  : Apply an arbitrary affine transform
- [`ga_rotate_around_center()`](https://josiahparry.github.io/geoarrowrs/reference/ga_rotate_around_center.md)
  : Rotate around the bounding box center
- [`ga_rotate_around_centroid()`](https://josiahparry.github.io/geoarrowrs/reference/ga_rotate_around_centroid.md)
  : Rotate geometries around their centroid
- [`ga_scale_xy()`](https://josiahparry.github.io/geoarrowrs/reference/ga_scale_xy.md)
  : Scale geometries in x and y
- [`ga_skew()`](https://josiahparry.github.io/geoarrowrs/reference/ga_skew.md)
  : Skew geometries uniformly about their centroid
- [`ga_skew_xy()`](https://josiahparry.github.io/geoarrowrs/reference/ga_skew_xy.md)
  : Skew geometries in x and y
- [`ga_translate()`](https://josiahparry.github.io/geoarrowrs/reference/ga_translate.md)
  : Translate geometries along the x and y axes
- [`ga_simplify()`](https://josiahparry.github.io/geoarrowrs/reference/ga_simplify.md)
  : Simplify with Ramer-Douglas-Peucker
- [`ga_simplify_idx()`](https://josiahparry.github.io/geoarrowrs/reference/ga_simplify_idx.md)
  [`ga_simplify_vw_idx()`](https://josiahparry.github.io/geoarrowrs/reference/ga_simplify_idx.md)
  : Find which coordinates simplification would keep
- [`ga_simplify_vw()`](https://josiahparry.github.io/geoarrowrs/reference/ga_simplify_vw.md)
  [`ga_simplify_vw_preserve()`](https://josiahparry.github.io/geoarrowrs/reference/ga_simplify_vw.md)
  : Simplify with Visvalingam-Whyatt
- [`ga_to_degrees()`](https://josiahparry.github.io/geoarrowrs/reference/ga_to_degrees.md)
  : Convert coordinates from radians to degrees
- [`ga_to_radians()`](https://josiahparry.github.io/geoarrowrs/reference/ga_to_radians.md)
  : Convert coordinates from degrees to radians

## Interpolate

- [`ga_interpolate_point()`](https://josiahparry.github.io/geoarrowrs/reference/ga_interpolate_point.md)
  : Interpolate a point along a linestring
- [`ga_points_along_line()`](https://josiahparry.github.io/geoarrowrs/reference/ga_points_along_line.md)
  : Points at regular intervals along a line
- [`ga_point_at_distance_between()`](https://josiahparry.github.io/geoarrowrs/reference/interpolate_between.md)
  [`ga_point_at_ratio_between()`](https://josiahparry.github.io/geoarrowrs/reference/interpolate_between.md)
  : Interpolate a point between two points
- [`ga_densify()`](https://josiahparry.github.io/geoarrowrs/reference/ga_densify.md)
  : Add points so no segment exceeds a length

## Winding

- [`ga_is_ccw()`](https://josiahparry.github.io/geoarrowrs/reference/ga_is_ccw.md)
  [`ga_is_cw()`](https://josiahparry.github.io/geoarrowrs/reference/ga_is_ccw.md)
  : Test the winding order of a ring
- [`ga_orient()`](https://josiahparry.github.io/geoarrowrs/reference/ga_orient.md)
  : Apply a winding direction to polygon rings
- [`ga_winding_order()`](https://josiahparry.github.io/geoarrowrs/reference/ga_winding_order.md)
  : Determine the winding order of a ring

## Inspect

- [`ga_coords()`](https://josiahparry.github.io/geoarrowrs/reference/ga_coords.md)
  [`ga_exterior_coords()`](https://josiahparry.github.io/geoarrowrs/reference/ga_coords.md)
  : Collect a geometry's coordinates as points
- [`ga_lines()`](https://josiahparry.github.io/geoarrowrs/reference/ga_lines.md)
  : Split geometries into their line segments
- [`ga_n_coords()`](https://josiahparry.github.io/geoarrowrs/reference/ga_n_coords.md)
  : Count the coordinates in each geometry
- [`ga_is_valid()`](https://josiahparry.github.io/geoarrowrs/reference/validation.md)
  [`ga_validation_error()`](https://josiahparry.github.io/geoarrowrs/reference/validation.md)
  : Test whether geometries are well formed

## Cast

- [`ga_as_point()`](https://josiahparry.github.io/geoarrowrs/reference/ga_as_point.md)
  [`ga_as_linestring()`](https://josiahparry.github.io/geoarrowrs/reference/ga_as_point.md)
  [`ga_as_polygon()`](https://josiahparry.github.io/geoarrowrs/reference/ga_as_point.md)
  [`ga_as_multipoint()`](https://josiahparry.github.io/geoarrowrs/reference/ga_as_point.md)
  [`ga_as_multilinestring()`](https://josiahparry.github.io/geoarrowrs/reference/ga_as_point.md)
  [`ga_as_multipolygon()`](https://josiahparry.github.io/geoarrowrs/reference/ga_as_point.md)
  : Cast to one GeoArrow geometry type
- [`ga_cast_geometry()`](https://josiahparry.github.io/geoarrowrs/reference/ga_cast_geometry.md)
  : Cast geometries to another GeoArrow geometry type
- [`ga_downcast_geometry()`](https://josiahparry.github.io/geoarrowrs/reference/ga_downcast_geometry.md)
  : Cast to the narrowest geometry type
- [`ga_explode()`](https://josiahparry.github.io/geoarrowrs/reference/ga_explode.md)
  : Split multi-part geometries into their parts
- [`ga_flatten()`](https://josiahparry.github.io/geoarrowrs/reference/ga_flatten.md)
  : Collapse a list of geometries into a single array
- [`ga_from_wkb()`](https://josiahparry.github.io/geoarrowrs/reference/ga_from_wkb.md)
  : Read well known binary as a GeoArrow array

## Misc

- [`ga_buffer()`](https://josiahparry.github.io/geoarrowrs/reference/ga_buffer.md)
  : Buffer geometries by a given distance
- [`ga_centroid()`](https://josiahparry.github.io/geoarrowrs/reference/ga_centroid.md)
  : Compute the centroid of geometries
- [`ga_chaikin_smoothing()`](https://josiahparry.github.io/geoarrowrs/reference/ga_chaikin_smoothing.md)
  : Smooth geometries using the Chaikin algorithm
- [`ga_line_segmentize()`](https://josiahparry.github.io/geoarrowrs/reference/ga_line_segmentize.md)
  [`ga_line_segmentize_haversine()`](https://josiahparry.github.io/geoarrowrs/reference/ga_line_segmentize.md)
  : Split linestrings into equal segments
- [`ga_remove_repeated_points()`](https://josiahparry.github.io/geoarrowrs/reference/ga_remove_repeated_points.md)
  : Remove repeated consecutive points from geometries
- [`register_geoarrow_udfs()`](https://josiahparry.github.io/geoarrowrs/reference/register_geoarrow_udfs.md)
  : Register geoarrowrs functions with Arrow
