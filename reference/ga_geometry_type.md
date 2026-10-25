# Determine the type of each geometry

Returns the simple features type name of each geometry, such as
`"POINT"` or `"MULTIPOLYGON"`. `ga_geometry_type()` uses the same names
as
[`sf::st_geometry_type()`](https://r-spatial.github.io/sf/reference/st_geometry_type.html).

## Usage

``` r
ga_geometry_type(geometry)
```

## Arguments

- geometry:

  a GeoArrow geometry array

## Value

a string array of the same length as `geometry`

## Details

`ga_geometry_type()` returns `NA` for a null geometry. It does the same
for an empty point, which has no coordinates to read a geometry from.

## See also

Other geometry type:
[`ga_is()`](https://josiahparry.github.io/geoarrowrs/reference/ga_is.md)

## Examples

``` r
g <- geoarrow::as_geoarrow_array(sf::st_sfc(
  sf::st_point(c(0, 0)),
  sf::st_linestring(rbind(c(0, 0), c(1, 1)))
))

as.vector(ga_geometry_type(g))
#> [1] "POINT"      "LINESTRING"
```
