# Rotate around the bounding box center

Rotates each geometry counter-clockwise by `degrees` about the center of
its own axis-aligned bounding rectangle. The geometry type of the output
matches the geometry type of the input.

## Usage

``` r
ga_rotate_around_center(geometry, degrees)
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
[`ga_rotate_around_centroid()`](https://josiahparry.github.io/geoarrowrs/reference/ga_rotate_around_centroid.md),
[`ga_scale_xy()`](https://josiahparry.github.io/geoarrowrs/reference/ga_scale_xy.md),
[`ga_skew()`](https://josiahparry.github.io/geoarrowrs/reference/ga_skew.md),
[`ga_skew_xy()`](https://josiahparry.github.io/geoarrowrs/reference/ga_skew_xy.md),
[`ga_translate()`](https://josiahparry.github.io/geoarrowrs/reference/ga_translate.md)

## Examples

``` r
# the same triangle, turned about its bounding box center instead
tri <- geoarrow::as_geoarrow_array(sf::st_sfc(
  sf::st_polygon(list(rbind(c(0, 0), c(4, 0), c(0, 3), c(0, 0))))
))

geoarrow::as_geoarrow_vctr(ga_rotate_around_center(tri, 90))
#> <geoarrow_vctr geoarrow.polygon{list}[1]>
#> [1] <POLYGON ((3.5 -0.5, 3.5 3.5, 0.5 -0.5, 3.5 -0.5))>
```
