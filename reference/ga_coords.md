# Collect a geometry's coordinates as points

Returns one multipoint per input geometry, holding that geometry's
vertices in order. The output has the same length as the input.

## Usage

``` r
ga_coords(geometry)

ga_exterior_coords(geometry)
```

## Arguments

- geometry:

  a GeoArrow geometry array

## Value

a GeoArrow multipoint array of the same length as `geometry`

## Details

`ga_exterior_coords()` skips the interior rings of a polygon, so it
gives the outline only. Both preserve the order the coordinates are
stored in, so a closed ring repeats its first vertex at the end.

A null geometry stays null.

## References

[CoordsIter](https://docs.rs/geo/latest/geo/algorithm/coords_iter/trait.CoordsIter.html)

## See also

Other iteration:
[`ga_lines()`](https://josiahparry.github.io/geoarrowrs/reference/ga_lines.md),
[`ga_n_coords()`](https://josiahparry.github.io/geoarrowrs/reference/ga_n_coords.md)

## Examples

``` r
ring <- matrix(c(0, 0, 2, 0, 2, 2, 0, 2, 0, 0), ncol = 2, byrow = TRUE)
g <- geoarrow::as_geoarrow_array(sf::st_sfc(sf::st_polygon(list(ring))))

sf::st_as_sfc(geoarrow::as_geoarrow_vctr(ga_coords(g)))
#> Geometry set for 1 feature 
#> Geometry type: MULTIPOINT
#> Dimension:     XY
#> Bounding box:  xmin: 0 ymin: 0 xmax: 2 ymax: 2
#> CRS:           NA
#> MULTIPOINT ((0 0), (2 0), (2 2), (0 2), (0 0))
as.vector(ga_n_coords(g))
#> [1] 5
```
