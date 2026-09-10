# Split linestrings into a given number of equal-length segments

Divides each linestring into `segment_count` segments of equal length,
returning a multilinestring. Returns `NA` if segmentation fails.
`line_segmentize_haversine()` uses the Haversine formula for geographic
coordinates; `line_segmentize()` uses planar Euclidean distance.

## Usage

``` r
line_segmentize(geometry, segment_count)

line_segmentize_haversine(geometry, segment_count)
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
[`buffer()`](https://josiahparry.github.io/geoarrowrs/reference/buffer.md),
[`centroid()`](https://josiahparry.github.io/geoarrowrs/reference/centroid.md),
[`chaikin_smoothing()`](https://josiahparry.github.io/geoarrowrs/reference/chaikin_smoothing.md),
[`remove_repeated_points()`](https://josiahparry.github.io/geoarrowrs/reference/remove_repeated_points.md)
