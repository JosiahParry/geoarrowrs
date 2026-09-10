# Skew geometries uniformly about their centroid

Shears each geometry by `degrees` along both the x and y dimensions,
about the geometry's own centroid. The geometry type of the output
matches the geometry type of the input.

## Usage

``` r
skew(geometry, degrees)
```

## Arguments

- geometry:

  a GeoArrow point, linestring, multilinestring, polygon, or
  multipolygon array

- degrees:

  the shear angle; length 1 or the same length as `geometry`

## Value

a GeoArrow array of the same geometry type as the input

## References

[Skew](https://docs.rs/geo/latest/geo/algorithm/skew/trait.Skew.html)

## See also

Other affine:
[`rotate_around_center()`](https://josiahparry.github.io/geoarrowrs/reference/rotate_around_center.md),
[`rotate_around_centroid()`](https://josiahparry.github.io/geoarrowrs/reference/rotate_around_centroid.md),
[`scale_xy()`](https://josiahparry.github.io/geoarrowrs/reference/scale_xy.md),
[`skew_xy()`](https://josiahparry.github.io/geoarrowrs/reference/skew_xy.md),
[`translate()`](https://josiahparry.github.io/geoarrowrs/reference/translate.md)
