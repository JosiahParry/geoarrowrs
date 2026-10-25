# Determine the winding order of a ring

Returns `"clockwise"` or `"counterclockwise"` for each geometry. For a
polygon, `ga_winding_order()` reports the winding of the exterior ring.

## Usage

``` r
ga_winding_order(geometry)
```

## Arguments

- geometry:

  a GeoArrow linestring, polygon, or multi part array of either

## Value

a string array of the same length as `geometry`

## Details

`ga_winding_order()` reports a winding for a multipolygon only when
every part's exterior ring agrees, and for a multilinestring only when
every part agrees; it returns `NA` for a geometry whose parts disagree,
since that geometry has no single winding. It also returns `NA` for any
other geometry type, a null geometry, or a ring with fewer than three
distinct points.

## References

[Winding](https://docs.rs/geo/latest/geo/algorithm/winding_order/trait.Winding.html)

## See also

Other winding:
[`ga_is_ccw()`](https://josiahparry.github.io/geoarrowrs/reference/ga_is_ccw.md),
[`ga_orient()`](https://josiahparry.github.io/geoarrowrs/reference/ga_orient.md),
[`ga_reverse()`](https://josiahparry.github.io/geoarrowrs/reference/ga_reverse.md)

## Examples

``` r
ring <- matrix(c(0, 0, 0, 1, 1, 1, 1, 0, 0, 0), ncol = 2, byrow = TRUE)
g <- geoarrow::as_geoarrow_array(sf::st_sfc(sf::st_polygon(list(ring))))

as.vector(ga_winding_order(g))
#> [1] "clockwise"
```
