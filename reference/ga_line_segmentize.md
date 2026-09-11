# Split linestrings into equal segments

Divides each linestring into `segment_count` segments of equal length,
returning a multilinestring. Returns `NA` if segmentation fails.
`ga_line_segmentize_haversine()` uses the Haversine formula for
geographic coordinates; `ga_line_segmentize()` uses planar Euclidean
distance.

## Usage

``` r
ga_line_segmentize(geometry, segment_count)

ga_line_segmentize_haversine(geometry, segment_count)
```

## Arguments

- geometry:

  a GeoArrow linestring array

- segment_count:

  the number of segments to split each linestring into; must be greater
  than 0

## Value

a GeoArrow multilinestring array

## References

[LineStringSegmentize](https://docs.rs/geo/latest/geo/algorithm/linestring_segment/trait.LineStringSegmentize.html)

[LineStringSegmentizeHaversine](https://docs.rs/geo/latest/geo/algorithm/linestring_segment/trait.LineStringSegmentizeHaversine.html)

## See also

Other misc:
[`ga_buffer()`](https://josiahparry.github.io/geoarrowrs/reference/ga_buffer.md),
[`ga_centroid()`](https://josiahparry.github.io/geoarrowrs/reference/ga_centroid.md),
[`ga_chaikin_smoothing()`](https://josiahparry.github.io/geoarrowrs/reference/ga_chaikin_smoothing.md),
[`ga_remove_repeated_points()`](https://josiahparry.github.io/geoarrowrs/reference/ga_remove_repeated_points.md)

## Examples

``` r
line <- geoarrow::as_geoarrow_array(sf::st_sfc(
  sf::st_linestring(rbind(c(0, 0), c(9, 0)))
))

# one multilinestring of three equal pieces
geoarrow::as_geoarrow_vctr(ga_line_segmentize(line, 3))
#> <geoarrow_vctr geoarrow.multilinestring{list}[1]>
#> [1] <MULTILINESTRING ((0 0, 3 0), (3 0, 6 0), (6 0, 9 0))>
```
