# Test whether a ring is convex

`TRUE` when the geometry's ring turns consistently in one direction.
`ga_is_convex()` judges a polygon on its exterior ring.

## Usage

``` r
ga_is_convex(geometry)
```

## Arguments

- geometry:

  a GeoArrow linestring or polygon array

## Value

a boolean array of the same length as `geometry`

## Details

`ga_is_convex()` reports a linestring as convex only when it is closed,
that is forms a ring, and returns `FALSE` for an open one. A polygon's
exterior ring is always closed, so `ga_is_convex()` needs no special
case for them. `ga_is_convex()` returns `NA` rather than `FALSE` for any
other geometry type or a null geometry.

## References

[IsConvex](https://docs.rs/geo/latest/geo/algorithm/is_convex/trait.IsConvex.html)

## See also

Other query:
[`ga_closest_point()`](https://josiahparry.github.io/geoarrowrs/reference/ga_closest_point.md),
[`ga_interior_point()`](https://josiahparry.github.io/geoarrowrs/reference/ga_interior_point.md),
[`ga_line_locate_point()`](https://josiahparry.github.io/geoarrowrs/reference/ga_line_locate_point.md)

## Examples

``` r
square <- sf::st_polygon(list(
  matrix(c(0, 0, 1, 0, 1, 1, 0, 1, 0, 0), ncol = 2, byrow = TRUE)
))
g <- geoarrow::as_geoarrow_array(sf::st_sfc(square))

as.vector(ga_is_convex(g))
#> [1] TRUE
```
