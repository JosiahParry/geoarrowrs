# Rotate geometries around the center of their bounding box

Rotates each geometry counter-clockwise by `degrees` about the center of
its own axis-aligned bounding rectangle. The geometry type of the output
matches the geometry type of the input.

## Usage

``` r
rotate_around_center(geometry, degrees)
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
[`affine_transform()`](https://josiahparry.github.io/geoarrowrs/reference/affine_transform.md),
[`rotate_around_centroid()`](https://josiahparry.github.io/geoarrowrs/reference/rotate_around_centroid.md),
[`scale_xy()`](https://josiahparry.github.io/geoarrowrs/reference/scale_xy.md),
[`skew()`](https://josiahparry.github.io/geoarrowrs/reference/skew.md),
[`skew_xy()`](https://josiahparry.github.io/geoarrowrs/reference/skew_xy.md),
[`translate()`](https://josiahparry.github.io/geoarrowrs/reference/translate.md)
