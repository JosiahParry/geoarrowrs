# Compute the extreme coordinates of geometries

Returns a struct array with four point fields – `x_min`, `x_max`,
`y_min` and `y_max` – giving the coordinate that is furthest in each
direction. The result has one row per input geometry; a null or empty
geometry yields a row of four nulls.

## Usage

``` r
extremes(x)
```

## Arguments

- x:

  a GeoArrow geometry array

## Value

an Arrow struct array of four point fields, with one row per geometry

## Details

Note these are the extreme *coordinates*, not the corners of the
bounding box: the `x_min` point carries the y value of whichever vertex
was leftmost. Use
[`bounding_rect()`](https://josiahparry.github.io/geoarrowrs/reference/bounding_rect.md)
for the envelope.

## References

[Extremes](https://docs.rs/geo/latest/geo/algorithm/extremes/trait.Extremes.html)

## See also

Other boundary:
[`bounding_rect()`](https://josiahparry.github.io/geoarrowrs/reference/bounding_rect.md),
[`concave_hull()`](https://josiahparry.github.io/geoarrowrs/reference/concave_hull.md),
[`convex_hull()`](https://josiahparry.github.io/geoarrowrs/reference/convex_hull.md),
[`minimum_rotated_rect()`](https://josiahparry.github.io/geoarrowrs/reference/minimum_rotated_rect.md)
