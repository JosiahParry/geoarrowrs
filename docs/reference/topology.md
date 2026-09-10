# Test a topological relationship between two geometry arrays

Each function compares `x` and `y` row by row and returns `TRUE` when
the named DE-9IM relationship holds. `y` is recycled against `x`.

## Usage

``` r
contains(x, y)

contains_properly(x, y)

within(x, y)

covers(x, y)

covered_by(x, y)

intersects(x, y)

disjoint(x, y)

touches(x, y)

crosses(x, y)

overlaps(x, y)

equals_topo(x, y)
```

## Arguments

- x:

  a GeoArrow geometry array

- y:

  a GeoArrow geometry array; length 1 or the same length as `x`

## Value

a boolean array of the same length as `x`

## Details

`contains()` is `TRUE` when no point of `y` lies outside `x` and at
least one point of `y` lies in the interior of `x`. `within()` is the
same test with the arguments swapped. `covers()` and `covered_by()` are
the weaker forms that allow every shared point to lie on the boundary.
`contains_properly()` is the stricter form requiring `y` to fall
entirely within the interior.

`intersects()` and `disjoint()` are negations of one another.
`touches()` is `TRUE` when the geometries share a boundary point but no
interior point, `crosses()` when their interiors meet in a lower
dimension than at least one of them, and `overlaps()` when they meet in
the same dimension as both. `equals_topo()` compares point sets rather
than coordinate order, so two geometries wound differently are still
equal.

A null geometry on either side gives `NA`.

## References

[Relate](https://docs.rs/geo/latest/geo/algorithm/relate/trait.Relate.html)

## See also

Other topology:
[`coordinate_position()`](https://josiahparry.github.io/geoarrowrs/reference/coordinate_position.md),
[`dimension()`](https://josiahparry.github.io/geoarrowrs/reference/dimension.md),
[`is_empty()`](https://josiahparry.github.io/geoarrowrs/reference/is_empty.md),
[`relate()`](https://josiahparry.github.io/geoarrowrs/reference/relate.md)

## Examples

``` r
big <- sf::st_polygon(list(
  matrix(c(0, 0, 4, 0, 4, 4, 0, 4, 0, 0), ncol = 2, byrow = TRUE)
))
x <- geoarrow::as_geoarrow_array(sf::st_sfc(big))
y <- geoarrow::as_geoarrow_array(sf::st_sfc(sf::st_point(c(2, 2))))

as.vector(nanoarrow::convert_array(contains(x, y)))
#> Error in check_match(match): `match` must be a character vector of non empty strings.
as.vector(nanoarrow::convert_array(intersects(x, y)))
#> [1] TRUE
```
