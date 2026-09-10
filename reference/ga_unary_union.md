# Dissolve an entire array of polygons into one

Merges every polygon in `x` into a single multipolygon, dropping the
boundaries between any that touch or overlap.

## Usage

``` r
ga_unary_union(x)
```

## Arguments

- x:

  a GeoArrow polygon or multipolygon array

## Value

a GeoArrow multipolygon array of length 1

## Details

This is the one function in the package that is not length-preserving.
It is an aggregate over the whole array, so it always returns a length 1
array, in the way [`sum()`](https://rdrr.io/r/base/sum.html) reduces a
vector to a single value.

It is far faster than folding
[`ga_boolean_union()`](https://josiahparry.github.io/geoarrowrs/reference/boolean_ops.md)
across the array, since it unions all the rings in one pass. Rows that
are not polygonal, and null rows, are skipped.

## References

[unary_union](https://docs.rs/geo/latest/geo/algorithm/bool_ops/fn.unary_union.html)

## See also

Other boolean:
[`ga_boolean_intersection()`](https://josiahparry.github.io/geoarrowrs/reference/boolean_ops.md)

## Examples

``` r
bx <- function(a, b, c, d) sf::st_polygon(list(matrix(
  c(a, b, c, b, c, d, a, d, a, b), ncol = 2, byrow = TRUE
)))
x <- geoarrow::as_geoarrow_array(sf::st_sfc(bx(0, 0, 2, 2), bx(1, 1, 3, 3)))

sf::st_area(sf::st_as_sfc(geoarrow::as_geoarrow_vctr(ga_unary_union(x))))
#> [1] 7
```
