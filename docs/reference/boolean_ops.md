# Combine two polygon arrays with a set operation

Each function pairs `x` with `y` row by row and returns the polygonal
result. `y` is recycled against `x`.

## Usage

``` r
boolean_intersection(x, y)

boolean_union(x, y)

boolean_difference(x, y)

boolean_xor(x, y)
```

## Arguments

- x:

  a GeoArrow polygon or multipolygon array

- y:

  a GeoArrow polygon or multipolygon array; length 1 or the same length
  as `x`

## Value

a GeoArrow multipolygon array of the same length as `x`

## Details

`boolean_intersection()` keeps the area in both, `boolean_union()` the
area in either, `boolean_difference()` the area in `x` but not `y`, and
`boolean_xor()` the area in exactly one of them.

These are defined for polygons and multipolygons only. A row whose
geometry is any other type, or is null, comes back null. The result is
always a multipolygon, since a set operation can split one polygon into
several or erase it entirely.

## References

[BooleanOps](https://docs.rs/geo/latest/geo/algorithm/bool_ops/trait.BooleanOps.html)

## See also

Other boolean:
[`unary_union()`](https://josiahparry.github.io/geoarrowrs/reference/unary_union.md)

## Examples

``` r
bx <- function(a, b, c, d) sf::st_polygon(list(matrix(
  c(a, b, c, b, c, d, a, d, a, b), ncol = 2, byrow = TRUE
)))
x <- geoarrow::as_geoarrow_array(sf::st_sfc(bx(0, 0, 2, 2)))
y <- geoarrow::as_geoarrow_array(sf::st_sfc(bx(1, 1, 3, 3)))

sf::st_area(sf::st_as_sfc(geoarrow::as_geoarrow_vctr(
  boolean_intersection(x, y)
)))
#> [1] 1
```
