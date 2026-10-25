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

Unlike most functions in the package, `ga_unary_union()` does not
preserve length. It aggregates over the whole array, so it always
returns a length 1 array, the way
[`sum()`](https://rdrr.io/r/base/sum.html) reduces a vector to a single
value.

It is far faster than folding
[`ga_boolean_union()`](https://josiahparry.github.io/geoarrowrs/reference/boolean_ops.md)
across the array, since it unions all the rings in one pass. It skips
rows that are not polygonal, and null rows.

## References

[unary_union](https://docs.rs/geo/latest/geo/algorithm/bool_ops/fn.unary_union.html)

## See also

Other boolean:
[`ga_boolean_intersection()`](https://josiahparry.github.io/geoarrowrs/reference/boolean_ops.md)

Other aggregate:
[`ga_collect_agg()`](https://josiahparry.github.io/geoarrowrs/reference/ga_collect_agg.md),
[`ga_interpolate_aw()`](https://josiahparry.github.io/geoarrowrs/reference/ga_interpolate_aw.md)

## Examples

``` r
x <- geoarrow::as_geoarrow_array(sf::st_sfc(
  sf::st_polygon(list(rbind(c(0, 0), c(2, 0), c(2, 2), c(0, 2), c(0, 0)))),
  sf::st_polygon(list(rbind(c(1, 1), c(3, 1), c(3, 3), c(1, 3), c(1, 1))))
))

# 4 + 4 less the 1 they share
as.vector(ga_unsigned_area(ga_unary_union(x)))
#> [1] 7
```
