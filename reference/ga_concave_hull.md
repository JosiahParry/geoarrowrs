# Compute the concave hull of geometries

Returns a polygon enclosing each geometry, and tightens that polygon
toward the geometry's own shape. With smaller `concavity` values,
`ga_concave_hull()` produces a tighter, more concave hull; with larger
values, it approaches the convex hull.

## Usage

``` r
ga_concave_hull(geometry, concavity, length_threshold)
```

## Arguments

- geometry:

  a GeoArrow multipoint, linestring, multilinestring, polygon, or
  multipolygon array

- concavity:

  the concavity coefficient; length 1 or the same length as `geometry`.
  `geo` uses 2 by default

- length_threshold:

  the length below which `ga_concave_hull()` stops refining an edge;
  length 1 or the same length as `geometry`. `geo` uses 0 by default

## Value

a GeoArrow polygon array

## Details

Accepts multipoints, linestrings, multilinestrings, polygons, and
multipolygons. `geo` does not define a concave hull for a single point.

## References

[ConcaveHull](https://docs.rs/geo/latest/geo/algorithm/concave_hull/trait.ConcaveHull.html)

## See also

Other boundary:
[`ga_convex_hull()`](https://josiahparry.github.io/geoarrowrs/reference/ga_convex_hull.md),
[`ga_envelope()`](https://josiahparry.github.io/geoarrowrs/reference/ga_envelope.md),
[`ga_extremes()`](https://josiahparry.github.io/geoarrowrs/reference/ga_extremes.md),
[`ga_minimum_rotated_rect()`](https://josiahparry.github.io/geoarrowrs/reference/ga_minimum_rotated_rect.md)

## Examples

``` r
fp <- system.file("shape/nc.shp", package = "sf")
nc <- as.data.frame(read_shapefile(fp))

# with lower concavity, ga_concave_hull() tightens the hull onto the shape
loose <- ga_concave_hull(nc$geometry, concavity = 2, length_threshold = 0)
tight <- ga_concave_hull(nc$geometry, concavity = 0.5, length_threshold = 0)

head(as.vector(ga_unsigned_area(loose)), 3)
#> [1] 0.11502123 0.06249588 0.12857049
head(as.vector(ga_unsigned_area(tight)), 3)
#> [1] 0.11428350 0.02801177 0.08427341
```
