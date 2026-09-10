# Skew geometries independently in x and y about their centroid

Shears each geometry by `degrees_x` along the x dimension and
`degrees_y` along the y dimension, about the geometry's own centroid.
The geometry type of the output matches the geometry type of the input.

## Usage

``` r
ga_skew_xy(geometry, degrees_x, degrees_y)
```

## Arguments

- geometry:

  a GeoArrow point, linestring, multilinestring, polygon, or
  multipolygon array

- degrees_x:

  shear angle along the x dimension; length 1 or the same length as
  `geometry`

- degrees_y:

  shear angle along the y dimension; length 1 or the same length as
  `geometry`

## Value

a GeoArrow array of the same geometry type as the input

## References

[Skew](https://docs.rs/geo/latest/geo/algorithm/skew/trait.Skew.html)

## See also

Other affine:
[`ga_affine_transform()`](https://josiahparry.github.io/geoarrowrs/reference/ga_affine_transform.md),
[`ga_rotate_around_center()`](https://josiahparry.github.io/geoarrowrs/reference/ga_rotate_around_center.md),
[`ga_rotate_around_centroid()`](https://josiahparry.github.io/geoarrowrs/reference/ga_rotate_around_centroid.md),
[`ga_scale_xy()`](https://josiahparry.github.io/geoarrowrs/reference/ga_scale_xy.md),
[`ga_skew()`](https://josiahparry.github.io/geoarrowrs/reference/ga_skew.md),
[`ga_translate()`](https://josiahparry.github.io/geoarrowrs/reference/ga_translate.md)
