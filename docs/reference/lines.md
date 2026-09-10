# Split geometries into their line segments

Returns one multilinestring per input geometry, whose parts are that
geometry's two point segments. The output has the same length as the
input.

## Usage

``` r
lines(geometry)
```

## Arguments

- geometry:

  a GeoArrow geometry array

## Value

a GeoArrow multilinestring array of the same length as `geometry`

## Details

A polygon contributes the segments of every ring, interior rings
included. A point has no segments and becomes a null element, as does a
null geometry.

## References

[LinesIter](https://docs.rs/geo/latest/geo/algorithm/lines_iter/trait.LinesIter.html)

## See also

Other iteration:
[`coords()`](https://josiahparry.github.io/geoarrowrs/reference/coords.md),
[`n_coords()`](https://josiahparry.github.io/geoarrowrs/reference/n_coords.md)

## Examples

``` r
ring <- matrix(c(0, 0, 2, 0, 2, 2, 0, 2, 0, 0), ncol = 2, byrow = TRUE)
g <- geoarrow::as_geoarrow_array(sf::st_sfc(sf::st_polygon(list(ring))))

lengths(sf::st_as_sfc(geoarrow::as_geoarrow_vctr(lines(g))))
#> [1] 4
```
