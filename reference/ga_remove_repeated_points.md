# Remove repeated consecutive points from geometries

Removes consecutive duplicate coordinates from each geometry. Accepts
linestrings, multilinestrings, polygons, and multipolygons.

## Usage

``` r
ga_remove_repeated_points(geometry)
```

## Arguments

- geometry:

  a GeoArrow linestring, multilinestring, polygon, or multipolygon array

## Value

a GeoArrow array of the same geometry type as the input

## References

[RemoveRepeatedPoints](https://docs.rs/geo/latest/geo/algorithm/remove_repeated_points/trait.RemoveRepeatedPoints.html)

## See also

Other misc:
[`ga_buffer()`](https://josiahparry.github.io/geoarrowrs/reference/ga_buffer.md),
[`ga_centroid()`](https://josiahparry.github.io/geoarrowrs/reference/ga_centroid.md),
[`ga_chaikin_smoothing()`](https://josiahparry.github.io/geoarrowrs/reference/ga_chaikin_smoothing.md),
[`ga_line_segmentize()`](https://josiahparry.github.io/geoarrowrs/reference/ga_line_segmentize.md)

## Examples

``` r
# the middle vertex is recorded twice
line <- geoarrow::as_geoarrow_array(sf::st_sfc(
  sf::st_linestring(rbind(c(0, 0), c(1, 1), c(1, 1), c(2, 2)))
))

as.vector(ga_n_coords(line))
#> [1] 4
as.vector(ga_n_coords(ga_remove_repeated_points(line)))
#> [1] 3
```
