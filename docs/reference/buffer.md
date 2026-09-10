# Buffer geometries by a given distance

Expands each geometry outward by `distance` to produce a multipolygon.
Line cap and join styles control the shape of the buffer at endpoints
and corners.

## Usage

``` r
buffer(geometry, distance, line_cap, line_join, miter_limit, round_segments)
```

## Arguments

- geometry:

  a GeoArrow geometry array

- distance:

  a numeric vector of buffer distances; length 1 or the same length as
  `geometry`

- line_cap:

  one of `"round"`, `"square"`, or `"butt"`

- line_join:

  one of `"round"`, `"miter"`, or `"bevel"`

- miter_limit:

  the miter limit used when `line_join` is `"miter"`

- round_segments:

  the number of segments used to approximate curves when `line_cap` or
  `line_join` is `"round"`

## Value

a GeoArrow multipolygon array

## References

[Buffer](https://docs.rs/geo/latest/geo/algorithm/buffer/trait.Buffer.html)

## See also

Other misc:
[`centroid()`](https://josiahparry.github.io/geoarrowrs/reference/centroid.md),
[`chaikin_smoothing()`](https://josiahparry.github.io/geoarrowrs/reference/chaikin_smoothing.md),
[`line_segmentize()`](https://josiahparry.github.io/geoarrowrs/reference/line_segmentize.md),
[`remove_repeated_points()`](https://josiahparry.github.io/geoarrowrs/reference/remove_repeated_points.md)
