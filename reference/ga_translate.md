# Translate geometries along the x and y axes

Shifts every coordinate of each geometry by `x_offset` and `y_offset`.
`ga_translate()` keeps the input's geometry type.

## Usage

``` r
ga_translate(geometry, x_offset, y_offset)
```

## Arguments

- geometry:

  a GeoArrow geometry array

- x_offset:

  distance to shift along the x axis; length 1 or the same length as
  `geometry`

- y_offset:

  distance to shift along the y axis; length 1 or the same length as
  `geometry`

## Value

a GeoArrow array of the same geometry type as the input

## Details

`ga_translate()` accepts any geometry type, including multipoints,
geometry collections, and WKB. It returns a null for a null geometry or
a null offset.

## References

[Translate](https://docs.rs/geo/latest/geo/algorithm/translate/trait.Translate.html)

## See also

Other affine:
[`ga_affine_transform()`](https://josiahparry.github.io/geoarrowrs/reference/ga_affine_transform.md),
[`ga_jitter()`](https://josiahparry.github.io/geoarrowrs/reference/ga_jitter.md),
[`ga_rotate_around_center()`](https://josiahparry.github.io/geoarrowrs/reference/ga_rotate_around_center.md),
[`ga_rotate_around_centroid()`](https://josiahparry.github.io/geoarrowrs/reference/ga_rotate_around_centroid.md),
[`ga_scale_xy()`](https://josiahparry.github.io/geoarrowrs/reference/ga_scale_xy.md),
[`ga_skew()`](https://josiahparry.github.io/geoarrowrs/reference/ga_skew.md),
[`ga_skew_xy()`](https://josiahparry.github.io/geoarrowrs/reference/ga_skew_xy.md)

## Examples

``` r
pts <- ga_point(c(0, 1), c(0, 1))

# ga_translate() shifts each point by its own row's offsets
geoarrow::as_geoarrow_vctr(ga_translate(pts, c(10, 100), c(0, 5)))
#> <geoarrow_vctr geoarrow.point{struct}[2]>
#> [1] <POINT (10 0)>  <POINT (101 6)>
```
