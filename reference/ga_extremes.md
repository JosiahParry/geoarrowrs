# Compute the extreme coordinates of geometries

Returns a struct array with four point fields – `x_min`, `x_max`,
`y_min` and `y_max` – giving the coordinate that is furthest in each
direction. The result has one row per input geometry; a null or empty
geometry yields a row of four nulls.

## Usage

``` r
ga_extremes(x)
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
[`ga_bounding_rect()`](https://josiahparry.github.io/geoarrowrs/reference/ga_bounding_rect.md)
for the envelope.

## References

[Extremes](https://docs.rs/geo/latest/geo/algorithm/extremes/trait.Extremes.html)

## See also

Other boundary:
[`ga_bounding_rect()`](https://josiahparry.github.io/geoarrowrs/reference/ga_bounding_rect.md),
[`ga_concave_hull()`](https://josiahparry.github.io/geoarrowrs/reference/ga_concave_hull.md),
[`ga_convex_hull()`](https://josiahparry.github.io/geoarrowrs/reference/ga_convex_hull.md),
[`ga_minimum_rotated_rect()`](https://josiahparry.github.io/geoarrowrs/reference/ga_minimum_rotated_rect.md)
