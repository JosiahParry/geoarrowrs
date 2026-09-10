# Split linestrings into a given number of equal-length segments

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
