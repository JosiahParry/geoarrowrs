# Test whether a ring is convex

`TRUE` when the geometry's ring turns consistently in one direction. A
polygon is judged on its exterior ring.

## Usage

``` r
is_convex(geometry)
```

## Arguments

- geometry:

  a GeoArrow linestring or polygon array

## Value

a boolean array of the same length as `geometry`

## Details

A linestring must be closed, that is form a ring, to be reported convex;
an open one is `FALSE`. A polygon's exterior ring is always closed, so
polygons need no special handling. Any other geometry type, or a null
geometry, becomes `NA` rather than `FALSE`.

## References

[IsConvex](https://docs.rs/geo/latest/geo/algorithm/is_convex/trait.IsConvex.html)

## See also

Other query:
[`closest_point()`](https://josiahparry.github.io/geoarrowrs/reference/closest_point.md),
[`interior_point()`](https://josiahparry.github.io/geoarrowrs/reference/interior_point.md),
[`line_locate_point()`](https://josiahparry.github.io/geoarrowrs/reference/line_locate_point.md)

## Examples

``` r
square <- sf::st_polygon(list(
  matrix(c(0, 0, 1, 0, 1, 1, 0, 1, 0, 0), ncol = 2, byrow = TRUE)
))
g <- geoarrow::as_geoarrow_array(sf::st_sfc(square))

as.vector(nanoarrow::convert_array(is_convex(g)))
#> [1] TRUE
```
