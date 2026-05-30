
- Implements the algorithms in the `geo` crate on geoarrow arrays using the geo-traits compatibility 


## Operations on Metric Spaces

- ⚠️ (needs matrix and self) Distance: Calculate the minimum distance between two geometries.
- ✅ Length: Calculate the length of a Line, LineString, or MultiLineString.
- ✅ Bearing: Calculate the bearing between two points.
- ✅ Destination: Calculate the destination point from an origin point, given a bearing and a distance.
- ✅ InterpolateLine: Interpolate a Point along a Line or LineString.
- ✅ InterpolatePoint: Interpolate points along a line.
- ✅ Densify: Insert points into a geometry so there is never more than max_segment_length between points.

## Misc measures

At present only pairwise distances are implementd. We need to also implement dense matrices and self-distance square matrices.

- ⚠️ (needs matrix and self) HausdorffDistance: Calculate "the maximum of the distances from a point in any of the sets to the nearest point in the other set." (Rote, 1991)
- ⚠️ (needs matrix and self)  VincentyDistance: Calculate the minimum geodesic distance between geometries using Vincenty’s formula
- ⚠️ (needs matrix and self)  VincentyLength: Calculate the geodesic length of a geometry using Vincenty’s formula
- ⚠️ (needs matrix and self) FrechetDistance: Calculate the similarity between LineStrings using the Fréchet distance

## Area

- ✅ Area: Calculate the planar area of a geometry
- ✅ ChamberlainDuquetteArea: Calculate the geodesic area of a geometry on a sphere using the algorithm presented in Some Algorithms for Polygons on a Sphere by Chamberlain and Duquette (2007)
- ✅  GeodesicArea: Calculate the geodesic area and perimeter of a geometry on an ellipsoid using the algorithm presented in Algorithms for geodesics by Charles Karney (2013)

## Boolean Operations

Note that Boolean ops on array will likely require query trees to be effective and scalable.
This will require more work and should be pushed to the end of implementation.

- BooleanOps: Combine or split (Multi)Polygons using intersection, union, xor, or difference operations
- unary_union: Efficient union of many Polygon or MultiPolygons
- Outlier Detection / Clustering
- OutlierDetection: Detect outliers in a group of points using LOF
- Dbscan: Calculate point clusters using the DBSCAN algorithm
- KMeans: Calculate point clusters using the k-means algorithm

## Simplification

- ✅ Simplify: Simplify a geometry using the Ramer–Douglas–Peucker algorithm
- SimplifyIdx: Calculate a simplified geometry using the Ramer–Douglas–Peucker algorithm, returning coordinate indices
- ✅ SimplifyVw: Simplify a geometry using the Visvalingam-Whyatt algorithm
- ✅ SimplifyVwPreserve: Simplify a geometry using a topology-preserving variant of the Visvalingam-Whyatt algorithm
- SimplifyVwIdx: Calculate a simplified geometry using the Visvalingam-Whyatt algorithm, returning coordinate indices

## Query

- ClosestPoint: Find the point on a geometry closest to a given point
- HaversineClosestPoint: Find the point on a geometry closest to a given point on a sphere using spherical coordinates and lines being great arcs
- IsConvex: Calculate the convexity of a LineString
- LineLocatePoint: Calculate the fraction of a line’s total length representing the location of the closest point on the line to the given point
- InteriorPoint: Calculates a representative point inside a Geometry

## Topology

- Contains: Calculate if a geometry contains another geometry
- ContainsProperly: Calculate if a geometry completely contains another geometry within its interior
- CoordinatePosition: Calculate the position of a coordinate relative to a geometry
- Covers: Calculate if a geometry covers another geometry
- HasDimensions: Determine the dimensions of a geometry
- Intersects: Calculate if a geometry intersects another geometry
- line_intersection: Calculates the intersection, if any, between two lines
- Intersections: Find all line segment intersections using an efficient sweep line algorithm (Bentley-Ottmann)
- Relate: Topologically relate two geometries based on DE-9IM semantics
- Within: Calculate if a geometry lies completely within another geometry

## Triangulation

- TriangulateEarcut: Triangulate polygons using the earcut algorithm. Requires the earcutr feature, which is enabled by default
- TriangulateDelaunay: Produce constrained or unconstrained Delaunay triangulations of polygons. Requires the spade feature, which is enabled by default
- Voronoi: Produce the Voronoi Diagram of a triangulation

## Winding

- Orient: Apply a specified winding Direction to a Polygon’s interior and exterior rings
- Winding: Calculate and manipulate the WindingOrder of a LineString

## Iteration

CoordsIter: Iterate over the coordinates of a geometry
MapCoords: Map a function over all the coordinates in a geometry, returning a new geometry
MapCoordsInPlace: Map a function over all the coordinates in a geometry in-place
LinesIter: Iterate over lines of a geometry

## Boundary

- BoundingRect: Calculate the axis-aligned bounding rectangle of a geometry
- MinimumRotatedRect: Calculate the minimum bounding box of a geometry
- ConcaveHull: Calculate the concave hull of a geometry
- ConvexHull: Calculate the convex hull of a geometry
- Extremes: Calculate the extreme coordinates and indices of a geometry
- Affine transformations
- Rotate: Rotate a geometry around its centroid
- Scale: Scale a geometry up or down by a factor
- Skew: Skew a geometry by shearing angles along the x and y dimension
- Translate: Translate a geometry along its axis
- AffineOps: generalised composable affine operations

## Conversion

- Convert: Convert (infallibly) the numeric type of a geometry’s coordinate value
- TryConvert: Convert (fallibly) the numeric type of a geometry’s coordinate value
- ToDegrees: Radians to degrees coordinate transforms for a given geometry
- ToRadians: Degrees to radians coordinate transforms for a given geometry

## Miscellaneous

- Buffer: Create a new geometry whose boundary is offset the specified distance from the input.
- Centroid: Calculate the centroid of a geometry
- ChaikinSmoothing: Smoothen LineString, Polygon, MultiLineString and MultiPolygon using Chaikin’s algorithm
- [proj]: Project geometries with the proj crate (requires the proj feature)
- LineStringSegmentize: Segment a LineString into n segments
- LineStringSegmentizeHaversine: Segment a LineString using Haversine distance
- [Transform]: Transform a geometry using Proj
- RemoveRepeatedPoints: Remove repeated points from a geometry
- Validation: Checks if the geometry is well formed. Some algorithms may not work correctly with invalid geometries
