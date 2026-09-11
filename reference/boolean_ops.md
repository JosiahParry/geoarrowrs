# Combine two polygon arrays with a set operation

Each function pairs `x` with `y` row by row and returns the polygonal
result. `y` is recycled against `x`.

## Usage

``` r
ga_boolean_intersection(x, y)

ga_boolean_union(x, y)

ga_boolean_difference(x, y)

ga_boolean_xor(x, y)
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

`ga_boolean_intersection()` keeps the area in both, `ga_boolean_union()`
the area in either, `ga_boolean_difference()` the area in `x` but not
`y`, and `ga_boolean_xor()` the area in exactly one of them.

These are defined for polygons and multipolygons only. A row whose
geometry is any other type, or is null, comes back null. The result is
always a multipolygon, since a set operation can split one polygon into
several or erase it entirely.

## References

[BooleanOps](https://docs.rs/geo/latest/geo/algorithm/bool_ops/trait.BooleanOps.html)

## See also

Other boolean:
[`ga_unary_union()`](https://josiahparry.github.io/geoarrowrs/reference/ga_unary_union.md)

## Examples

``` r
x <- geoarrow::as_geoarrow_array(sf::st_sfc(
  sf::st_polygon(list(rbind(c(0, 0), c(2, 0), c(2, 2), c(0, 2), c(0, 0))))
))
y <- geoarrow::as_geoarrow_array(sf::st_sfc(
  sf::st_polygon(list(rbind(c(1, 1), c(3, 1), c(3, 3), c(1, 3), c(1, 1))))
))

# two 2x2 squares offset by 1, so the overlap is a 1x1 square
as.vector(ga_unsigned_area(ga_boolean_intersection(x, y)))
#> [1] 1
```
