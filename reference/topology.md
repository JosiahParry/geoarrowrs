# Test a topological relationship between two geometry arrays

Each function compares `x` and `y` row by row and returns `TRUE` when
the named DE-9IM relationship holds. `y` is recycled against `x`.

## Usage

``` r
ga_contains(x, y)

ga_contains_properly(x, y)

ga_within(x, y)

ga_covers(x, y)

ga_covered_by(x, y)

ga_intersects(x, y)

ga_disjoint(x, y)

ga_touches(x, y)

ga_crosses(x, y)

ga_overlaps(x, y)

ga_equals_topo(x, y)
```

## Arguments

- x:

  a GeoArrow geometry array

- y:

  a GeoArrow geometry array; length 1 or the same length as `x`

## Value

a boolean array of the same length as `x`

## Details

`ga_contains()` is `TRUE` when no point of `y` lies outside `x` and at
least one point of `y` lies in the interior of `x`. `ga_within()` is the
same test with the arguments swapped. `ga_covers()` and
`ga_covered_by()` are the weaker forms that allow every shared point to
lie on the boundary. `ga_contains_properly()` is the stricter form
requiring `y` to fall entirely within the interior.

`ga_intersects()` and `ga_disjoint()` are negations of one another.
`ga_touches()` is `TRUE` when the geometries share a boundary point but
no interior point, `ga_crosses()` when their interiors meet in a lower
dimension than at least one of them, and `ga_overlaps()` when they meet
in the same dimension as both. `ga_equals_topo()` compares point sets
rather than coordinate order, so two geometries wound differently are
still equal.

A null geometry on either side gives `NA`.

## References

[Relate](https://docs.rs/geo/latest/geo/algorithm/relate/trait.Relate.html)

## See also

Other topology:
[`ga_coordinate_position()`](https://josiahparry.github.io/geoarrowrs/reference/ga_coordinate_position.md),
[`ga_dimension()`](https://josiahparry.github.io/geoarrowrs/reference/ga_dimension.md),
[`ga_is_empty()`](https://josiahparry.github.io/geoarrowrs/reference/ga_is_empty.md),
[`ga_relate()`](https://josiahparry.github.io/geoarrowrs/reference/ga_relate.md)

## Examples

``` r
big <- sf::st_polygon(list(
  matrix(c(0, 0, 4, 0, 4, 4, 0, 4, 0, 0), ncol = 2, byrow = TRUE)
))
x <- geoarrow::as_geoarrow_array(sf::st_sfc(big))
y <- geoarrow::as_geoarrow_array(sf::st_sfc(sf::st_point(c(2, 2))))

as.vector(ga_contains(x, y))
#> [1] TRUE
as.vector(ga_intersects(x, y))
#> [1] TRUE
```
