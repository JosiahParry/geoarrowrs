# Determine the topological dimension of geometries

Returns 0 for points, 1 for lines and curves, and 2 for surfaces.

## Usage

``` r
ga_dimension(geometry)

ga_boundary_dimension(geometry)
```

## Arguments

- geometry:

  a GeoArrow geometry array

## Value

an integer array of the same length as `geometry`

## Details

An empty geometry has no dimension and gives `NA`, which is distinct
from a point's 0. `ga_boundary_dimension()` gives the dimension of the
geometry's boundary instead, so a polygon is 1 and a point is `NA`.

## References

[HasDimensions](https://docs.rs/geo/latest/geo/algorithm/dimensions/trait.HasDimensions.html)

## See also

Other topology:
[`ga_contains()`](https://josiahparry.github.io/geoarrowrs/reference/topology.md),
[`ga_coordinate_position()`](https://josiahparry.github.io/geoarrowrs/reference/ga_coordinate_position.md),
[`ga_filter()`](https://josiahparry.github.io/geoarrowrs/reference/ga_filter.md),
[`ga_is_empty()`](https://josiahparry.github.io/geoarrowrs/reference/ga_is_empty.md),
[`ga_join()`](https://josiahparry.github.io/geoarrowrs/reference/ga_join.md),
[`ga_relate()`](https://josiahparry.github.io/geoarrowrs/reference/ga_relate.md),
[`ga_sparse_intersects()`](https://josiahparry.github.io/geoarrowrs/reference/sparse.md),
[`ga_sparse_pairs()`](https://josiahparry.github.io/geoarrowrs/reference/ga_sparse_pairs.md)

## Examples

``` r
g <- geoarrow::as_geoarrow_array(sf::st_sfc(
  sf::st_point(c(0, 0)),
  sf::st_linestring(cbind(c(0, 1), c(0, 1)))
))

as.vector(ga_dimension(g))
#> [1] 0 1
```
