# Translate geometries along the x and y axes

Shifts every coordinate of each geometry by `x_offset` and `y_offset`.
The geometry type of the output matches the geometry type of the input.

## Usage

``` r
translate(geometry, x_offset, y_offset)
```

## Arguments

- geometry:

  a GeoArrow point, linestring, multilinestring, polygon, or
  multipolygon array

- x_offset:

  distance to shift along the x axis; length 1 or the same length as
  `geometry`

- y_offset:

  distance to shift along the y axis; length 1 or the same length as
  `geometry`

## Value

a GeoArrow array of the same geometry type as the input

## References

[Translate](https://docs.rs/geo/latest/geo/algorithm/translate/trait.Translate.html)

## See also

Other affine:
[`affine_transform()`](https://josiahparry.github.io/geoarrowrs/reference/affine_transform.md),
[`rotate_around_center()`](https://josiahparry.github.io/geoarrowrs/reference/rotate_around_center.md),
[`rotate_around_centroid()`](https://josiahparry.github.io/geoarrowrs/reference/rotate_around_centroid.md),
[`scale_xy()`](https://josiahparry.github.io/geoarrowrs/reference/scale_xy.md),
[`skew()`](https://josiahparry.github.io/geoarrowrs/reference/skew.md),
[`skew_xy()`](https://josiahparry.github.io/geoarrowrs/reference/skew_xy.md)
