# Compute the concave hull of geometries

Returns a polygon enclosing each geometry, tightened toward the
geometry's own shape. Smaller `concavity` values produce a tighter, more
concave hull; larger values approach the convex hull.

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

  edges shorter than this are not considered for further refinement;
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
[`ga_bounding_rect()`](https://josiahparry.github.io/geoarrowrs/reference/ga_bounding_rect.md),
[`ga_convex_hull()`](https://josiahparry.github.io/geoarrowrs/reference/ga_convex_hull.md),
[`ga_extremes()`](https://josiahparry.github.io/geoarrowrs/reference/ga_extremes.md),
[`ga_minimum_rotated_rect()`](https://josiahparry.github.io/geoarrowrs/reference/ga_minimum_rotated_rect.md)
