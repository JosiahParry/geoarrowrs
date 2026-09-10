# geoarrowrs

Implements the algorithms in the `geo` crate on GeoArrow arrays via the
`geo-traits` compatibility layer, plus readers for common spatial file formats.

Every exported function is vectorized and length-preserving: `n` geometries in,
`n` out. Numeric arguments accept a plain R vector or an Arrow array and are
recycled against the geometry length. Readers return a record batch stream;
everything else returns an Arrow or GeoArrow array.

Legend: ✅ done · ⚠️ partial · ❌ not started

## IO

- ✅ `read_shapefile()`: Read a shapefile, including its `.dbf` attributes. Z and M variants are detected from the coordinate data rather than the header, which is unreliable.
- ✅ `read_geojson()`: Read a GeoJSON file. Property columns keep document order and their types are widened across features.
- ✅ `read_flatgeobuf()`: Read a FlatGeobuf file. Streams, and takes an optional `bbox` served by the file's packed Hilbert R-tree.
- ❌ GeoParquet: covered by the `geoarrow` R package, so not planned here.

## Casting

- ✅ `cast_geometry()`: Cast between GeoArrow geometry types, including all variants to the mixed `geometry` type.
- ✅ `downcast_geometry()`: Narrow an array to the most specific type its contents allow.
- ✅ `explode()`: Split multi-part geometries into their parts. Length-preserving, returning one array of parts per row.
- ✅ `flatten()`: Collapse an exploded list array back into a flat array of parts.

## Operations on Metric Spaces

- ⚠️ Distance: pairwise done for the Euclidean, Haversine, geodesic and rhumb metrics (`dist_euclidean_pairwise()`, `dist_haversine_pairwise()`, `dist_geodesic_pairwise()`, `dist_rhumb_pairwise()`; points only). Dense and self-distance matrices are still needed.
- ✅ Length: Calculate the length of a Line, LineString, or MultiLineString (`length_euclidean()`, `length_haversine()`, `length_geodesic()`, `length_rhumb()`).
- ✅ Bearing: Calculate the bearing between two points (`bearing_euclidean()`, `bearing_haversine()`, `bearing_geodesic()`, `bearing_rhumb()`).
- ✅ Destination: Calculate the destination point from an origin point, given a bearing and a distance (`dest_euclidean()`, `dest_haversine()`, `dest_geodesic()`, `dest_rhumb()`).
- ✅ InterpolateLine: Interpolate a Point along a Line or LineString (`interpolate_point()`).
- ✅ InterpolatePoint: Interpolate points along a line (`point_at_distance_between()`, `point_at_ratio_between()`, `points_along_line()`).
- ✅ Densify: Insert points into a geometry so there is never more than max_segment_length between points (`densify()`).

## Misc measures

The distance methods here are pairwise only. Each still needs a dense matrix
form and a self-distance square matrix form. Lengths are unary and need neither.

- ⚠️ HausdorffDistance: pairwise done (`dist_hausdorff_pairwise()`), matrix and self forms pending.
- ⚠️ VincentyDistance: pairwise done (`dist_vincenty_pairwise()`, points only), matrix and self forms pending.
- ✅ VincentyLength: Calculate the geodesic length of a geometry using Vincenty's formula (`length_vincenty()`). Returns `NA` when the algorithm fails to converge.
- ⚠️ FrechetDistance: pairwise done (`dist_frechet_pairwise()`, linestrings only, Euclidean metric), matrix and self forms pending.

## Area

- ✅ Area: Calculate the planar area of a geometry (`signed_area()`, `unsigned_area()`).
- ✅ ChamberlainDuquetteArea: Calculate the geodesic area of a geometry on a sphere (`signed_area_cd()`, `unsigned_area_cd()`).
- ✅ GeodesicArea: Calculate the geodesic area and perimeter of a geometry on an ellipsoid (`signed_area_geodesic()`, `unsigned_area_geodesic()`, `perimeter_signed_geodesic()`, `perimeter_unsigned_geodesic()`).

## Boolean Operations

Note that Boolean ops on array will likely require query trees to be effective and scalable.
This will require more work and should be pushed to the end of implementation.

- ❌ BooleanOps: Combine or split (Multi)Polygons using intersection, union, xor, or difference operations
- ❌ unary_union: Efficient union of many Polygon or MultiPolygons
- ❌ Outlier Detection / Clustering
- ❌ OutlierDetection: Detect outliers in a group of points using LOF
- ❌ Dbscan: Calculate point clusters using the DBSCAN algorithm
- ❌ KMeans: Calculate point clusters using the k-means algorithm

## Simplification

- ✅ Simplify: Simplify a geometry using the Ramer-Douglas-Peucker algorithm (`simplify()`).
- ❌ SimplifyIdx: Calculate a simplified geometry using the Ramer-Douglas-Peucker algorithm, returning coordinate indices
- ✅ SimplifyVw: Simplify a geometry using the Visvalingam-Whyatt algorithm (`simplify_vw()`).
- ✅ SimplifyVwPreserve: Simplify a geometry using a topology-preserving variant of the Visvalingam-Whyatt algorithm (`simplify_vw_preserve()`).
- ❌ SimplifyVwIdx: Calculate a simplified geometry using the Visvalingam-Whyatt algorithm, returning coordinate indices

## Query

- ✅ ClosestPoint: Find the point on a geometry closest to a given point (`closest_point()`).
- ✅ HaversineClosestPoint: Find the point on a geometry closest to a given point on a sphere (`closest_point_haversine()`).
- ✅ IsConvex: Calculate the convexity of a LineString (`is_convex()`).
- ✅ LineLocatePoint: Calculate the fraction of a line's total length representing the location of the closest point on the line to the given point (`line_locate_point()`).
- ✅ InteriorPoint: Calculates a representative point inside a Geometry (`interior_point()`).

## Topology

- ❌ Contains: Calculate if a geometry contains another geometry
- ❌ ContainsProperly: Calculate if a geometry completely contains another geometry within its interior
- ❌ CoordinatePosition: Calculate the position of a coordinate relative to a geometry
- ❌ Covers: Calculate if a geometry covers another geometry
- ❌ HasDimensions: Determine the dimensions of a geometry
- ❌ Intersects: Calculate if a geometry intersects another geometry
- ❌ line_intersection: Calculates the intersection, if any, between two lines
- ❌ Intersections: Find all line segment intersections using an efficient sweep line algorithm (Bentley-Ottmann)
- ❌ Relate: Topologically relate two geometries based on DE-9IM semantics
- ❌ Within: Calculate if a geometry lies completely within another geometry

## Triangulation

- ✅ TriangulateEarcut: Triangulate polygons using the earcut algorithm (`triangulate_earcut()`).
- ✅ TriangulateDelaunay: Produce constrained or unconstrained Delaunay triangulations of polygons (`triangulate_delaunay()`).
- ✅ Voronoi: Produce the Voronoi Diagram of a triangulation (`voronoi_cells()`, `voronoi_edges()`).

The triangulate functions return one multipolygon of triangles per input
geometry, and the voronoi functions one multipolygon of cells or one
multilinestring of edges. All are length-preserving rather than one row per
triangle or cell. Use `explode()` to get at the individual parts.

## Winding

- ✅ Orient: Apply a specified winding Direction to a Polygon's interior and exterior rings (`orient()`).
- ✅ Winding: Calculate and manipulate the WindingOrder of a LineString (`winding_order()`, `is_ccw()`, `is_cw()`).

## Iteration

- ❌ CoordsIter: Iterate over the coordinates of a geometry
- ❌ MapCoords: Map a function over all the coordinates in a geometry, returning a new geometry
- ❌ MapCoordsInPlace: Map a function over all the coordinates in a geometry in-place
- ❌ LinesIter: Iterate over lines of a geometry

## Boundary

- ✅ BoundingRect: Calculate the axis-aligned bounding rectangle of a geometry (`bounding_rect()`).
- ✅ MinimumRotatedRect: Calculate the minimum bounding box of a geometry (`minimum_rotated_rect()`).
- ✅ ConcaveHull: Calculate the concave hull of a geometry (`concave_hull()`).
- ✅ ConvexHull: Calculate the convex hull of a geometry (`convex_hull()`).
- ✅ Extremes: Calculate the extreme coordinates and indices of a geometry (`extremes()`).
- Affine transformations
  - ✅ Rotate: Rotate a geometry around its centroid or its bounding rect centre (`rotate_around_centroid()`, `rotate_around_center()`).
  - ✅ Scale: Scale a geometry up or down by a factor (`scale_xy()`).
  - ✅ Skew: Skew a geometry by shearing angles along the x and y dimension (`skew()`, `skew_xy()`).
  - ✅ Translate: Translate a geometry along its axis (`translate()`).
  - ❌ AffineOps: generalised composable affine operations. Decided against exposing a matrix; this should take six recyclable vectors.

## Conversion

- ❌ Convert: not applicable. R has one numeric type, so coordinates are always f64.
- ❌ TryConvert: as above.
- ✅ ToDegrees: Radians to degrees coordinate transforms for a given geometry (`to_degrees()`).
- ✅ ToRadians: Degrees to radians coordinate transforms for a given geometry (`to_radians()`).

## Miscellaneous

- ✅ Buffer: Create a new geometry whose boundary is offset the specified distance from the input (`buffer()`).
- ✅ Centroid: Calculate the centroid of a geometry (`centroid()`).
- ✅ ChaikinSmoothing: Smoothen LineString, Polygon, MultiLineString and MultiPolygon using Chaikin's algorithm (`chaikin_smoothing()`).
- ❌ [proj]: Project geometries with the proj crate. Out of scope for now.
- ✅ LineStringSegmentize: Segment a LineString into n segments (`line_segmentize()`).
- ✅ LineStringSegmentizeHaversine: Segment a LineString using Haversine distance (`line_segmentize_haversine()`).
- ❌ [Transform]: Transform a geometry using Proj. Out of scope for now.
- ✅ RemoveRepeatedPoints: Remove repeated points from a geometry (`remove_repeated_points()`).
- ❌ Validation: Checks if the geometry is well formed. Some algorithms may not work correctly with invalid geometries

## Known limitations

- Z and M coordinates are dropped by every algorithm. The `geo` bridge goes
  through a 2D `geo::Geometry`. The readers do preserve them on the way in.
- `explode()` is driven by the array's declared type, not its contents. Call
  `downcast_geometry()` first if the array is typed as mixed `geometry`.
- `cast_geometry()` works around a `geoarrow-array` 0.8 bug: the `From` impls
  for LineString to MultiLineString and Polygon to MultiPolygon size
  `geom_offsets` by coordinate count rather than geometry count and panic.
  Not yet reported upstream.
- `voronoi_edges()` trips a `debug_assert!` in geo 0.33.1 on ordinary input.
  It is compiled out in release, so an installed package is fine, but the
  function errors under a debug build such as `devtools::test()`. Its tests
  skip on that specific error. Not yet reported upstream.
