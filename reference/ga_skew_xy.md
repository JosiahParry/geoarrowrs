# Skew geometries in x and y

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

## Examples

``` r
sq <- geoarrow::as_geoarrow_array(sf::st_sfc(
  sf::st_polygon(list(rbind(c(0, 0), c(2, 0), c(2, 2), c(0, 2), c(0, 0))))
))

# shear along x only, leaving y untouched
geoarrow::as_geoarrow_vctr(ga_skew_xy(sq, 30, 0))
#> <geoarrow_vctr geoarrow.polygon{list}[1]>
#> [1] <POLYGON ((-0.5773503 0, 1.4226497 0, 2.5773503 2, 0.5773503 2, -0.5773>
```
