# Extract parts of one type from geometry collections

Gathers the points, lines, or polygons of each geometry into a single
multipoint, multilinestring, or multipolygon. `ga_collection_extract()`
flattens nested collections and multi part members in order.

## Usage

``` r
ga_collection_extract(geometry, geometry_type = "POLYGON")
```

## Arguments

- geometry:

  a GeoArrow geometry array

- geometry_type:

  one of `"POLYGON"`, `"LINESTRING"`, or `"POINT"`

## Value

a GeoArrow multipolygon, multilinestring, or multipoint array of the
same length as `geometry`

## Details

Unlike
[`sf::st_collection_extract()`](https://r-spatial.github.io/sf/reference/st_collection_extract.html),
`ga_collection_extract()` keeps one row per input row. It returns a null
for a geometry with no parts of `geometry_type`, such as a linestring
when you ask for polygons, and for a null geometry.

## See also

Other cast:
[`ga_as_point()`](https://josiahparry.github.io/geoarrowrs/reference/ga_as_point.md),
[`ga_cast_geometry()`](https://josiahparry.github.io/geoarrowrs/reference/ga_cast_geometry.md),
[`ga_downcast_geometry()`](https://josiahparry.github.io/geoarrowrs/reference/ga_downcast_geometry.md),
[`ga_explode()`](https://josiahparry.github.io/geoarrowrs/reference/ga_explode.md),
[`ga_flatten()`](https://josiahparry.github.io/geoarrowrs/reference/ga_flatten.md),
[`ga_from_wkb()`](https://josiahparry.github.io/geoarrowrs/reference/ga_from_wkb.md)

## Examples

``` r
gc <- sf::st_geometrycollection(list(
  sf::st_point(c(5, 5)),
  sf::st_polygon(list(rbind(c(0, 0), c(1, 0), c(1, 1), c(0, 0))))
))
g <- geoarrow::as_geoarrow_array(sf::st_sfc(gc, sf::st_point(c(0, 0))))

# the point has no polygons, so its row is null
as.vector(ga_geometry_type(ga_collection_extract(g, "POLYGON")))
#> [1] "MULTIPOLYGON" NA            
```
