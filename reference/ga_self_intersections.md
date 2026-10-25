# Find where a geometry crosses itself

Returns the points at which a geometry's own segments intersect, one
multipoint per input geometry.

## Usage

``` r
ga_self_intersections(geometry)
```

## Arguments

- geometry:

  a GeoArrow geometry array

## Value

a GeoArrow multipoint array of the same length as `geometry`

## Details

Uses the Bentley-Ottmann sweep line, which finds all crossings in
roughly `n log n` rather than by testing every pair. Use
`ga_self_intersections()` to locate the problem that
[`ga_is_valid()`](https://josiahparry.github.io/geoarrowrs/reference/validation.md)
reports: it returns the offending points of a self intersecting polygon.

`ga_self_intersections()` does not count segments that merely share an
endpoint, as consecutive segments of a linestring always do. It returns
an empty multipoint rather than a null for a geometry with no crossings,
so you can distinguish an empty result from an unsupported geometry. It
returns null for points and null rows.

## References

[Intersections](https://docs.rs/geo/latest/geo/sweep/struct.Intersections.html)

## See also

Other intersection:
[`ga_line_intersection()`](https://josiahparry.github.io/geoarrowrs/reference/ga_line_intersection.md)

## Examples

``` r
ring <- rbind(c(0, 0), c(2, 2), c(2, 0), c(0, 2), c(0, 0))
bowtie <- sf::st_polygon(list(ring))
g <- geoarrow::as_geoarrow_array(sf::st_sfc(bowtie))

as.vector(ga_is_valid(g))
#> [1] FALSE
sf::st_as_sfc(geoarrow::as_geoarrow_vctr(ga_self_intersections(g)))
#> Geometry set for 1 feature 
#> Geometry type: MULTIPOINT
#> Dimension:     XY
#> Bounding box:  xmin: 1 ymin: 1 xmax: 1 ymax: 1
#> CRS:           NA
#> MULTIPOINT ((1 1))
```
