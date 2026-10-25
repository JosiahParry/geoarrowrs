# Apply an arbitrary affine transform

Transforms every coordinate by the six coefficients of an affine matrix.
`ga_affine_transform()` keeps the input's geometry type.

## Usage

``` r
ga_affine_transform(geometry, a, b, xoff, d, e, yoff)
```

## Arguments

- geometry:

  a GeoArrow point, linestring, multilinestring, polygon, or
  multipolygon array

- a, b, d, e:

  the linear part of the matrix; each length 1 or the same length as
  `geometry`

- xoff, yoff:

  the translation part; each length 1 or the same length as `geometry`

## Value

a GeoArrow array of the same geometry type as `geometry`

## Details

`ga_affine_transform()` maps a coordinate to `x' = a * x + b * y + xoff`
and `y' = d * x + e * y + yoff`, the same ordering PostGIS `ST_Affine`
and Shapely use. Pass `a = 1, b = 0, xoff = 0, d = 0, e = 1, yoff = 0`
for the identity transform.

This is the general form behind
[`ga_translate()`](https://josiahparry.github.io/geoarrowrs/reference/ga_translate.md),
[`ga_scale_xy()`](https://josiahparry.github.io/geoarrowrs/reference/ga_scale_xy.md),
[`ga_rotate_around_centroid()`](https://josiahparry.github.io/geoarrowrs/reference/ga_rotate_around_centroid.md),
and
[`ga_skew()`](https://josiahparry.github.io/geoarrowrs/reference/ga_skew.md).
Reach for those when they fit, since they are clearer at the call site.
Use this one to apply a matrix you already have, or to compose several
steps into a single pass over the coordinates.

`ga_affine_transform()` recycles all six coefficients against
`geometry`, so you can apply a different transform to every row.

## References

[AffineOps](https://docs.rs/geo/latest/geo/algorithm/affine_ops/trait.AffineOps.html)

## See also

Other affine:
[`ga_jitter()`](https://josiahparry.github.io/geoarrowrs/reference/ga_jitter.md),
[`ga_rotate_around_center()`](https://josiahparry.github.io/geoarrowrs/reference/ga_rotate_around_center.md),
[`ga_rotate_around_centroid()`](https://josiahparry.github.io/geoarrowrs/reference/ga_rotate_around_centroid.md),
[`ga_scale_xy()`](https://josiahparry.github.io/geoarrowrs/reference/ga_scale_xy.md),
[`ga_skew()`](https://josiahparry.github.io/geoarrowrs/reference/ga_skew.md),
[`ga_skew_xy()`](https://josiahparry.github.io/geoarrowrs/reference/ga_skew_xy.md),
[`ga_translate()`](https://josiahparry.github.io/geoarrowrs/reference/ga_translate.md)

## Examples

``` r
ring <- rbind(c(0, 0), c(1, 0), c(1, 1), c(0, 1), c(0, 0))
square <- sf::st_polygon(list(ring))
g <- geoarrow::as_geoarrow_array(sf::st_sfc(square))

# double the width and shift right by 10
res <- ga_affine_transform(g, a = 2, b = 0, xoff = 10, d = 0, e = 1, yoff = 0)
sf::st_as_sfc(geoarrow::as_geoarrow_vctr(res))
#> Geometry set for 1 feature 
#> Geometry type: POLYGON
#> Dimension:     XY
#> Bounding box:  xmin: 10 ymin: 0 xmax: 12 ymax: 1
#> CRS:           NA
#> POLYGON ((10 0, 12 0, 12 1, 10 1, 10 0))
```
