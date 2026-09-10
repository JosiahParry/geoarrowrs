# Compute the DE-9IM relationship between two geometry arrays

Returns the nine character DE-9IM matrix describing how each pair of
geometries relates, from which every named predicate can be derived.

## Usage

``` r
ga_relate(x, y)
```

## Arguments

- x:

  a GeoArrow geometry array

- y:

  a GeoArrow geometry array; length 1 or the same length as `x`

## Value

a string array of the same length as `x`

## Details

The string reads as the intersections of the interior, boundary and
exterior of `x` with those of `y`, in that order. Each character is the
dimension of that intersection: `F` for empty, `0` for a point, `1` for
a curve, and `2` for a surface. A null geometry on either side gives
`NA`.

## References

[Relate](https://docs.rs/geo/latest/geo/algorithm/relate/trait.Relate.html)

## See also

Other topology:
[`ga_contains()`](https://josiahparry.github.io/geoarrowrs/reference/topology.md),
[`ga_coordinate_position()`](https://josiahparry.github.io/geoarrowrs/reference/ga_coordinate_position.md),
[`ga_dimension()`](https://josiahparry.github.io/geoarrowrs/reference/ga_dimension.md),
[`ga_is_empty()`](https://josiahparry.github.io/geoarrowrs/reference/ga_is_empty.md)

## Examples

``` r
big <- sf::st_polygon(list(
  matrix(c(0, 0, 4, 0, 4, 4, 0, 4, 0, 0), ncol = 2, byrow = TRUE)
))
x <- geoarrow::as_geoarrow_array(sf::st_sfc(big))
y <- geoarrow::as_geoarrow_array(sf::st_sfc(sf::st_point(c(2, 2))))

as.vector(ga_relate(x, y))
#> [1] "0F2FF1FF2"
```
