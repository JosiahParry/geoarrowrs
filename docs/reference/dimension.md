# Determine the topological dimension of geometries

Returns 0 for points, 1 for lines and curves, and 2 for surfaces.

## Usage

``` r
dimension(geometry)

boundary_dimension(geometry)
```

## Arguments

- geometry:

  a GeoArrow geometry array

## Value

an integer array of the same length as `geometry`

## Details

An empty geometry has no dimension and gives `NA`, which is distinct
from a point's 0. `boundary_dimension()` gives the dimension of the
geometry's boundary instead, so a polygon is 1 and a point is `NA`.

## References

[HasDimensions](https://docs.rs/geo/latest/geo/algorithm/dimensions/trait.HasDimensions.html)

## See also

Other topology:
[`contains()`](https://josiahparry.github.io/geoarrowrs/reference/topology.md),
[`coordinate_position()`](https://josiahparry.github.io/geoarrowrs/reference/coordinate_position.md),
[`is_empty()`](https://josiahparry.github.io/geoarrowrs/reference/is_empty.md),
[`relate()`](https://josiahparry.github.io/geoarrowrs/reference/relate.md)

## Examples

``` r
g <- geoarrow::as_geoarrow_array(sf::st_sfc(
  sf::st_point(c(0, 0)),
  sf::st_linestring(cbind(c(0, 1), c(0, 1)))
))

as.vector(nanoarrow::convert_array(dimension(g)))
#> [1] 0 1
```
