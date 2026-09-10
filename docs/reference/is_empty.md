# Test whether geometries are empty

`TRUE` when the geometry holds no coordinates.

## Usage

``` r
is_empty(geometry)
```

## Arguments

- geometry:

  a GeoArrow geometry array

## Value

a boolean array of the same length as `geometry`

## Details

An empty geometry is distinct from a null one. A null geometry gives
`NA`.

## References

[HasDimensions](https://docs.rs/geo/latest/geo/algorithm/dimensions/trait.HasDimensions.html)

## See also

Other topology:
[`contains()`](https://josiahparry.github.io/geoarrowrs/reference/topology.md),
[`coordinate_position()`](https://josiahparry.github.io/geoarrowrs/reference/coordinate_position.md),
[`dimension()`](https://josiahparry.github.io/geoarrowrs/reference/dimension.md),
[`relate()`](https://josiahparry.github.io/geoarrowrs/reference/relate.md)

## Examples

``` r
g <- geoarrow::as_geoarrow_array(sf::st_sfc(
  sf::st_point(c(0, 0)),
  sf::st_point()
))

as.vector(nanoarrow::convert_array(is_empty(g)))
#> [1] FALSE    NA
```
