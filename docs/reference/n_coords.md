# Count the coordinates in each geometry

Returns how many vertices each geometry holds.

## Usage

``` r
n_coords(geometry)
```

## Arguments

- geometry:

  a GeoArrow geometry array

## Value

an integer array of the same length as `geometry`

## Details

Counts every coordinate that
[`coords()`](https://josiahparry.github.io/geoarrowrs/reference/coords.md)
would return, so a closed ring counts its repeated final vertex. A null
geometry gives `NA`.

## References

[CoordsIter](https://docs.rs/geo/latest/geo/algorithm/coords_iter/trait.CoordsIter.html)

## See also

Other iteration:
[`coords()`](https://josiahparry.github.io/geoarrowrs/reference/coords.md),
[`lines()`](https://josiahparry.github.io/geoarrowrs/reference/lines.md)

## Examples

``` r
ring <- matrix(c(0, 0, 2, 0, 2, 2, 0, 2, 0, 0), ncol = 2, byrow = TRUE)
g <- geoarrow::as_geoarrow_array(sf::st_sfc(sf::st_polygon(list(ring))))

as.vector(nanoarrow::convert_array(n_coords(g)))
#> [1] 5
```
