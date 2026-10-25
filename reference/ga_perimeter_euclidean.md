# Planar perimeter of polygons

Sums the length of every ring, including interior rings, in the units of
the coordinate system. `ga_perimeter_euclidean()` returns 0 for points
and lines, which have no area.

## Usage

``` r
ga_perimeter_euclidean(x)
```

## Arguments

- x:

  a GeoArrow geometry array

## Value

a double array of the same length as `x`

## Details

Use
[`ga_perimeter_geodesic()`](https://josiahparry.github.io/geoarrowrs/reference/area_geodesic.md)
for geographic coordinates. `ga_perimeter_euclidean()` returns `NA` for
a null geometry.

## References

[Length](https://docs.rs/geo/latest/geo/algorithm/line_measures/trait.Length.html)

## See also

Other area:
[`ga_signed_area()`](https://josiahparry.github.io/geoarrowrs/reference/area.md),
[`ga_signed_area_cd()`](https://josiahparry.github.io/geoarrowrs/reference/area_cd.md),
[`ga_signed_area_geodesic()`](https://josiahparry.github.io/geoarrowrs/reference/area_geodesic.md)

## Examples

``` r
sq <- geoarrow::as_geoarrow_array(sf::st_sfc(
  sf::st_polygon(list(rbind(c(0, 0), c(2, 0), c(2, 2), c(0, 2), c(0, 0))))
))

as.vector(ga_perimeter_euclidean(sq))
#> [1] 8
```
