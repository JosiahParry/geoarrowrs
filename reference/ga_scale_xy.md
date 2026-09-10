# Scale geometries independently in x and y about their centroid

Scales each geometry by `x_factor` along the x axis and `y_factor` along
the y axis, about the geometry's own centroid. The geometry type of the
output matches the geometry type of the input.

## Usage

``` r
ga_scale_xy(geometry, x_factor, y_factor)
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
[`ga_affine_transform()`](https://josiahparry.github.io/geoarrowrs/reference/ga_affine_transform.md),
[`ga_rotate_around_center()`](https://josiahparry.github.io/geoarrowrs/reference/ga_rotate_around_center.md),
[`ga_rotate_around_centroid()`](https://josiahparry.github.io/geoarrowrs/reference/ga_rotate_around_centroid.md),
[`ga_skew()`](https://josiahparry.github.io/geoarrowrs/reference/ga_skew.md),
[`ga_skew_xy()`](https://josiahparry.github.io/geoarrowrs/reference/ga_skew_xy.md),
[`ga_translate()`](https://josiahparry.github.io/geoarrowrs/reference/ga_translate.md)
