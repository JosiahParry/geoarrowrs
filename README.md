
# geoarrowrs

`{geoarrowrs}` provides R bindings to the geoarrow-rs Rust crate. They
are in a very early stage.

## Readers

- [x] FlatGeoBuf
- [x] GeoJson
- [x] GeoJsonLines
- [x] GeoParquet ⚠️ Not all options implemented
- [x] Shapefile
- [ ] wkb - going to defer to `geoarrow-r`
- [ ] wkt - going to defer to `geoarrow-r`

## Writers

- [ ] FlatGeoBuf
- [ ] GeoJson
- [ ] GeoJsonLines
- [x] GeoParquet ⚠️ Not all options implemented
- [ ] Shapefile
- [ ] wkb - going to defer to `geoarrow-r`
- [ ] wkt - going to defer to `geoarrow-r`

## Algorithm Implementations:

### Geo

- [ ] AffineOps
- [x] Area
- [x] BoundingRect
- [x] Center
- [x] Centroid
- [x] ChaikinSmoothing
- [x] ChamberlainDuquetteArea
- [x] ConcaveHull - ⚠️ TODO allow an array of concavity
- [x] Contains
- [x] ConvexHull
- [x] Densify - ⚠️ TODO allow an array to densify
- [x] EuclideanDistance ⚠️ TODO dense matrix for distance
- [x] EuclideanLength
- [x] FrechetDistance ⚠️ TODO dense matrix for distance
- [ ] FrechetDistanceLineString - TODO i beleive this is for distance to
  a single other line
- [x] GeodesicArea - ⚠️ TODO geodesic_perimeter
- [x] GeodesicLength
- [x] HasDimensions
- [x] HaversineLength
- [x] InteriorPoint
- [ ] Intersects - ⚠️ not provided by geoarrow-rs yet
- [x] LineInterpolatePoint
- [x] LineLocatePoint
- [ ] LineLocatePointScalar - I’m unsure what this does
- [x] MinimumRotatedRect
- [x] RemoveRepeatedPoints
- [x] Rotate - ⚠️ missing `rotate_around_point()` unsure how to handle
  scalar geometry
- [x] Scale - ⚠️ missing `scale_around_point()` unsure how to handle
  scalar geometry
- [x] Simplify - ⚠️ TODO allow array for epsilon
- [x] SimplifyVw - ⚠️ TODO allow array for epsilon
- [x] SimplifyVwPreserve - ⚠️ TODO allow array for epsilon
- [x] Skew - ⚠️ TODO missing `skew_around_point()` idk how to handle
  scalar geometry
- [x] Translate
- [x] VincentyLength
- [x] Within

------------------------------------------------------------------------

Other traits that are missing from geoarrow-rs and I think should be
implemented there then ported to this package. This is not a complete
listing of the missing traits from geo.

- [ ] Kernel
- [ ] ClosestPoint
- [ ] CrossTrackDistance
- [ ] DensifyHaversine
- [ ] Extremes
- [ ] KNearestConcaveHull
- [ ] Geodesic::Bearing
- [ ] Geodesic::Distance
- [ ] Geodesic::Destination
- [ ] Geodesic::InterpolatePoint
- [ ] Haversine::Bearing
- [ ] Haversine::Destination
- [ ] Haversine::Distance
- [ ] Haversine::InterpolatePoint
- [ ] Rhumb::Bearing
- [ ] Rhumb::Destination
- [ ] Rhumb::Distance
- [ ] Rhumb::InterpolatePoint
- [ ] LineStringSegmentize
- [ ] LineStringSegmentizeHaversine
- [ ] Orient
- [ ] OutlierDetection
- [ ] TriangulateEarcut
- [ ] TriangulateSpade
- [ ] IsConvex
- [ ] Winding

### Native

- [ ] Binary
- [ ] Cast
- [ ] Concatenate
- [ ] Downcast
- [ ] DowncastTable
- [ ] Explode
- [ ] ExplodeTable
- [ ] MapChunks
- [ ] MapCoords
- [ ] Rechunk
- [ ] Take
- [ ] TotalBounds
- [ ] TypeIds
- [ ] Unary
- [ ] UnaryPoint

### rstar

- [ ] RTree

### geo_index

- [ ] RTree
