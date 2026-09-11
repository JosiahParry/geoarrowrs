# Locate a point relative to a geometry

Returns `"inside"`, `"outside"`, or `"boundary"` for each pair.

## Usage

``` r
ga_coordinate_position(geometry, point)
```

## Arguments

- geometry:

  a GeoArrow geometry array

- point:

  a GeoArrow point array; length 1 or the same length as `geometry`

## Value

a string array of the same length as `geometry`

## Details

This is the three way form of a point in polygon test, distinguishing a
point that lies exactly on an edge from one strictly inside. `point` is
recycled against `geometry`. A null geometry or a null point gives `NA`.

## References

[CoordinatePosition](https://docs.rs/geo/latest/geo/algorithm/coordinate_position/trait.CoordinatePosition.html)

## See also

Other topology:
[`ga_contains()`](https://josiahparry.github.io/geoarrowrs/reference/topology.md),
[`ga_dimension()`](https://josiahparry.github.io/geoarrowrs/reference/ga_dimension.md),
[`ga_is_empty()`](https://josiahparry.github.io/geoarrowrs/reference/ga_is_empty.md),
[`ga_join()`](https://josiahparry.github.io/geoarrowrs/reference/ga_join.md),
[`ga_relate()`](https://josiahparry.github.io/geoarrowrs/reference/ga_relate.md),
[`ga_sparse_intersects()`](https://josiahparry.github.io/geoarrowrs/reference/sparse.md)

## Examples

``` r
ring <- rbind(c(0, 0), c(4, 0), c(4, 4), c(0, 4), c(0, 0))
square <- sf::st_polygon(list(ring))
g <- geoarrow::as_geoarrow_array(sf::st_sfc(square, square, square))

# inside, on the edge, and outside
p <- ga_xy(c(2, 0, 9), c(2, 2, 9))
as.vector(ga_coordinate_position(g, p))
#> [1] "inside"   "boundary" "outside" 
```
