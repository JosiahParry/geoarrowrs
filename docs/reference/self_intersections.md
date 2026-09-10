# Find where a geometry crosses itself

Returns the points at which a geometry's own segments intersect, one
multipoint per input geometry.

## Usage

``` r
self_intersections(geometry)
```

## Arguments

- geometry:

  a GeoArrow geometry array

## Value

a GeoArrow multipoint array of the same length as `geometry`

## Details

Uses the Bentley-Ottmann sweep line, which finds all crossings in
roughly `n log n` rather than by testing every pair. This is how to
locate the problem that
[`is_valid()`](https://josiahparry.github.io/geoarrowrs/reference/validation.md)
reports: a self intersecting polygon comes back with the offending
points.

Segments that merely share an endpoint, as consecutive segments of a
linestring always do, are not counted. A geometry with no crossings
gives an empty multipoint rather than a null, so an empty result is
distinguishable from an unsupported geometry. Points and null rows come
back null.

## References

[Intersections](https://docs.rs/geo/latest/geo/sweep/struct.Intersections.html)

## See also

Other intersection:
[`line_intersection()`](https://josiahparry.github.io/geoarrowrs/reference/line_intersection.md)

## Examples

``` r
bowtie <- sf::st_polygon(list(matrix(
  c(0, 0, 2, 2, 2, 0, 0, 2, 0, 0), ncol = 2, byrow = TRUE
)))
g <- geoarrow::as_geoarrow_array(sf::st_sfc(bowtie))

as.vector(nanoarrow::convert_array(is_valid(g)))
#> [1] FALSE
sf::st_as_sfc(geoarrow::as_geoarrow_vctr(self_intersections(g)))
#> Geometry set for 1 feature 
#> Geometry type: MULTIPOINT
#> Dimension:     XY
#> Bounding box:  xmin: 1 ymin: 1 xmax: 1 ymax: 1
#> CRS:           NA
#> MULTIPOINT ((1 1))
```
