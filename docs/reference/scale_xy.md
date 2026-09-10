# Scale geometries independently in x and y about their centroid

Scales each geometry by `x_factor` along the x axis and `y_factor` along
the y axis, about the geometry's own centroid. The geometry type of the
output matches the geometry type of the input.

## Usage

``` r
scale_xy(geometry, x_factor, y_factor)
```

## Arguments

- geometry:

  a GeoArrow point, linestring, multilinestring, polygon, or
  multipolygon array

- x_factor:

  scaling factor along the x axis; length 1 or the same length as
  `geometry`

- y_factor:

  scaling factor along the y axis; length 1 or the same length as
  `geometry`

## Value

a GeoArrow array of the same geometry type as the input

## References

[Scale](https://docs.rs/geo/latest/geo/algorithm/scale/trait.Scale.html)

## See also

Other affine:
[`rotate_around_center()`](https://josiahparry.github.io/geoarrowrs/reference/rotate_around_center.md),
[`rotate_around_centroid()`](https://josiahparry.github.io/geoarrowrs/reference/rotate_around_centroid.md),
[`skew()`](https://josiahparry.github.io/geoarrowrs/reference/skew.md),
[`skew_xy()`](https://josiahparry.github.io/geoarrowrs/reference/skew_xy.md),
[`translate()`](https://josiahparry.github.io/geoarrowrs/reference/translate.md)
