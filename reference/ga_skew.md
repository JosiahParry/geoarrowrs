# Skew geometries uniformly about their centroid

Shears each geometry by `degrees` along both the x and y dimensions,
about the geometry's own centroid. The geometry type of the output
matches the geometry type of the input.

## Usage

``` r
ga_skew(geometry, degrees)
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
[`ga_affine_transform()`](https://josiahparry.github.io/geoarrowrs/reference/ga_affine_transform.md),
[`ga_rotate_around_center()`](https://josiahparry.github.io/geoarrowrs/reference/ga_rotate_around_center.md),
[`ga_rotate_around_centroid()`](https://josiahparry.github.io/geoarrowrs/reference/ga_rotate_around_centroid.md),
[`ga_scale_xy()`](https://josiahparry.github.io/geoarrowrs/reference/ga_scale_xy.md),
[`ga_skew_xy()`](https://josiahparry.github.io/geoarrowrs/reference/ga_skew_xy.md),
[`ga_translate()`](https://josiahparry.github.io/geoarrowrs/reference/ga_translate.md)

## Examples

``` r
sq <- geoarrow::as_geoarrow_array(sf::st_sfc(
  sf::st_polygon(list(rbind(c(0, 0), c(2, 0), c(2, 2), c(0, 2), c(0, 0))))
))

# a shear leaves the area unchanged
geoarrow::as_geoarrow_vctr(ga_skew(sq, 30))
#> <geoarrow_vctr geoarrow.polygon{list}[1]>
#> [1] <POLYGON ((-0.5773503 -0.5773503, 1.4226497 0.5773503, 2.5773503 2.5773>
as.vector(ga_unsigned_area(ga_skew(sq, 30)))
#> [1] 2.666667
```
