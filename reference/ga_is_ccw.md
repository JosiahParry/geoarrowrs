# Test the winding order of a ring

`ga_is_ccw()` is `TRUE` for counter-clockwise geometries and
`ga_is_cw()` is `TRUE` for clockwise ones. A polygon is tested on its
exterior ring.

## Usage

``` r
ga_is_ccw(geometry)

ga_is_cw(geometry)
```

## Arguments

- geometry:

  a GeoArrow linestring or polygon array

## Value

a boolean array of the same length as `geometry`

## Details

Any geometry with no winding order, such as a point or a null geometry,
becomes `NA` rather than `FALSE`, so the two functions are not simply
negations of one another.

## References

[Winding](https://docs.rs/geo/latest/geo/algorithm/winding_order/trait.Winding.html)

## See also

Other winding:
[`ga_orient()`](https://josiahparry.github.io/geoarrowrs/reference/ga_orient.md),
[`ga_winding_order()`](https://josiahparry.github.io/geoarrowrs/reference/ga_winding_order.md)

## Examples

``` r
ring <- matrix(c(0, 0, 0, 1, 1, 1, 1, 0, 0, 0), ncol = 2, byrow = TRUE)
g <- geoarrow::as_geoarrow_array(sf::st_sfc(sf::st_polygon(list(ring))))

as.vector(ga_is_ccw(g))
#> [1] FALSE
as.vector(ga_is_cw(ga_orient(g, "reversed")))
#> [1] TRUE
```
