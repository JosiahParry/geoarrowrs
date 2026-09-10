# Generate points at regular intervals along the line between two points

Returns a multipoint array where each element contains all points spaced
at most `max_distance` apart along the path from `start` to `end`.

## Usage

``` r
ga_points_along_line(start, end, max_distance, include_ends, metric)
```

## Arguments

- start:

  a GeoArrow point array of start points

- end:

  a GeoArrow point array of end points

- max_distance:

  the maximum spacing between generated points; length 1 or the same
  length as `start`

- include_ends:

  whether to include the start and end points in the output

- metric:

  one of `"euclidean"`, `"haversine"`, `"geodesic"`, or `"rhumb"`

## Value

a GeoArrow multipoint array

## References

[InterpolatePoint](https://docs.rs/geo/latest/geo/algorithm/line_measures/trait.InterpolatePoint.html)

## See also

Other interpolate:
[`ga_interpolate_point()`](https://josiahparry.github.io/geoarrowrs/reference/ga_interpolate_point.md),
[`ga_point_at_distance_between()`](https://josiahparry.github.io/geoarrowrs/reference/interpolate_between.md)
