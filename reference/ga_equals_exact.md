# Test whether geometries are identical within a tolerance

`TRUE` when `x` and `y` have the same type and structure and every
coordinate lies within `tolerance` of its counterpart.
`ga_equals_exact()` recycles `y` and `tolerance` against `x`.

## Usage

``` r
ga_equals_exact(x, y, tolerance = 0)
```

## Arguments

- x:

  a GeoArrow geometry array

- y:

  a GeoArrow geometry array; length 1 or the same length as `x`

- tolerance:

  the largest planar distance allowed between matching coordinates;
  length 1 or the same length as `x`

## Value

a boolean array of the same length as `x`

## Details

Unlike
[`ga_equals_topo()`](https://josiahparry.github.io/geoarrowrs/reference/topology.md),
`ga_equals_exact()` compares coordinates in order, so it does not treat
a line and its reverse as equal. It measures planar distance between
coordinates and returns `NA` for a null on either side.

## See also

Other topology:
[`ga_contains()`](https://josiahparry.github.io/geoarrowrs/reference/topology.md),
[`ga_coordinate_position()`](https://josiahparry.github.io/geoarrowrs/reference/ga_coordinate_position.md),
[`ga_dimension()`](https://josiahparry.github.io/geoarrowrs/reference/ga_dimension.md),
[`ga_filter()`](https://josiahparry.github.io/geoarrowrs/reference/ga_filter.md),
[`ga_is_empty()`](https://josiahparry.github.io/geoarrowrs/reference/ga_is_empty.md),
[`ga_join()`](https://josiahparry.github.io/geoarrowrs/reference/ga_join.md),
[`ga_relate()`](https://josiahparry.github.io/geoarrowrs/reference/ga_relate.md),
[`ga_sparse_equals_exact()`](https://josiahparry.github.io/geoarrowrs/reference/ga_sparse_equals_exact.md),
[`ga_sparse_intersects()`](https://josiahparry.github.io/geoarrowrs/reference/sparse.md),
[`ga_sparse_pairs()`](https://josiahparry.github.io/geoarrowrs/reference/ga_sparse_pairs.md)

## Examples

``` r
x <- geoarrow::as_geoarrow_array(sf::st_sfc(
  sf::st_linestring(rbind(c(0, 0), c(1, 1))),
  sf::st_linestring(rbind(c(1, 1), c(0, 0)))
))
y <- geoarrow::as_geoarrow_array(sf::st_sfc(
  sf::st_linestring(rbind(c(0, 0), c(1, 1.01)))
))

# ga_equals_exact() does not treat the reversed line as equal
as.vector(ga_equals_exact(x, y, 0.1))
#> [1]  TRUE FALSE
```
