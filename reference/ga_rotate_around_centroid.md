# Rotate geometries around their centroid

Rotates each geometry counter-clockwise by `degrees` about its own
centroid. The geometry type of the output matches the geometry type of
the input.

## Usage

``` r
ga_rotate_around_centroid(geometry, degrees)
```

## Arguments

- geometry:

  a GeoArrow point, linestring, multilinestring, polygon, or
  multipolygon array

- degrees:

  angle of rotation; length 1 or the same length as `geometry`

## Value

a GeoArrow array of the same geometry type as the input

## References

[Rotate](https://docs.rs/geo/latest/geo/algorithm/rotate/trait.Rotate.html)

## See also

Other affine:
[`ga_affine_transform()`](https://josiahparry.github.io/geoarrowrs/reference/ga_affine_transform.md),
[`ga_rotate_around_center()`](https://josiahparry.github.io/geoarrowrs/reference/ga_rotate_around_center.md),
[`ga_scale_xy()`](https://josiahparry.github.io/geoarrowrs/reference/ga_scale_xy.md),
[`ga_skew()`](https://josiahparry.github.io/geoarrowrs/reference/ga_skew.md),
[`ga_skew_xy()`](https://josiahparry.github.io/geoarrowrs/reference/ga_skew_xy.md),
[`ga_translate()`](https://josiahparry.github.io/geoarrowrs/reference/ga_translate.md)
