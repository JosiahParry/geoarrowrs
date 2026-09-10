# Determine the winding order of a ring

Returns `"clockwise"` or `"counterclockwise"` for each geometry. A
polygon reports the winding of its exterior ring.

## Usage

``` r
winding_order(geometry)
```

## Arguments

- geometry:

  a GeoArrow linestring, polygon, or multi part array of either

## Value

a string array of the same length as `geometry`

## Details

A multipolygon reports a winding only when every part's exterior ring
agrees, and a multilinestring only when every part agrees; a geometry
whose parts disagree is `NA`, since it has no single winding. Any other
geometry type, a null geometry, or a ring with fewer than three distinct
points is also `NA`.

## References

[Winding](https://docs.rs/geo/latest/geo/algorithm/winding_order/trait.Winding.html)

## See also

Other winding:
[`is_ccw()`](https://josiahparry.github.io/geoarrowrs/reference/is_ccw.md),
[`orient()`](https://josiahparry.github.io/geoarrowrs/reference/orient.md)

## Examples

``` r
ring <- matrix(c(0, 0, 0, 1, 1, 1, 1, 0, 0, 0), ncol = 2, byrow = TRUE)
g <- geoarrow::as_geoarrow_array(sf::st_sfc(sf::st_polygon(list(ring))))

as.vector(nanoarrow::convert_array(winding_order(g)))
#> [1] "clockwise"
```
