# Test whether geometries are of a given type

`TRUE` when the geometry's type is one of `types`. Type names are those
[`ga_geometry_type()`](https://josiahparry.github.io/geoarrowrs/reference/ga_geometry_type.md)
returns.

## Usage

``` r
ga_is(geometry, types)
```

## Arguments

- geometry:

  a GeoArrow geometry array

- types:

  a character vector of simple features type names, such as `"POLYGON"`

## Value

a boolean array of the same length as `geometry`

## Details

`ga_is()` returns `NA` for a null geometry. It errors on an unknown type
name rather than returning `FALSE`, so it never silently passes a
misspelling.

## See also

Other geometry type:
[`ga_geometry_type()`](https://josiahparry.github.io/geoarrowrs/reference/ga_geometry_type.md)

## Examples

``` r
g <- geoarrow::as_geoarrow_array(sf::st_sfc(
  sf::st_point(c(0, 0)),
  sf::st_linestring(rbind(c(0, 0), c(1, 1)))
))

as.vector(ga_is(g, "POINT"))
#> [1]  TRUE FALSE
as.vector(ga_is(g, c("POINT", "LINESTRING")))
#> [1] TRUE TRUE
```
