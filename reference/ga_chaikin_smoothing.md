# Smooth geometries using the Chaikin algorithm

Applies Chaikin's corner-cutting algorithm for the given number of
iterations to produce smoother curves. Accepts linestrings,
multilinestrings, polygons, and multipolygons.

## Usage

``` r
ga_chaikin_smoothing(geometry, n_iterations)
```

## Arguments

- geometry:

  a GeoArrow linestring, multilinestring, polygon, or multipolygon array

- n_iterations:

  the number of smoothing iterations to apply; must be greater than 0

## Value

a GeoArrow array of the same geometry type as the input

## References

[ChaikinSmoothing](https://docs.rs/geo/latest/geo/algorithm/chaikin_smoothing/trait.ChaikinSmoothing.html)

## See also

Other misc:
[`ga_buffer()`](https://josiahparry.github.io/geoarrowrs/reference/ga_buffer.md),
[`ga_centroid()`](https://josiahparry.github.io/geoarrowrs/reference/ga_centroid.md),
[`ga_line_segmentize()`](https://josiahparry.github.io/geoarrowrs/reference/ga_line_segmentize.md),
[`ga_remove_repeated_points()`](https://josiahparry.github.io/geoarrowrs/reference/ga_remove_repeated_points.md)

## Examples

``` r
zigzag <- geoarrow::as_geoarrow_array(sf::st_sfc(
  sf::st_linestring(rbind(c(0, 0), c(1, 2), c(2, 0), c(3, 2)))
))

# each iteration cuts every corner, so the vertex count grows
as.vector(ga_n_coords(zigzag))
#> [1] 4
as.vector(ga_n_coords(ga_chaikin_smoothing(zigzag, 2)))
#> [1] 16

geoarrow::as_geoarrow_vctr(ga_chaikin_smoothing(zigzag, 1))
#> <geoarrow_vctr geoarrow.linestring{list}[1]>
#> [1] <LINESTRING (0 0, 0.25 0.5, 0.75 1.5, 1.25 1.5, 1.75 0.5, 2.25 0.5, 2.7>
```
