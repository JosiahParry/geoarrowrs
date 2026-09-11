# Points at regular intervals along a line

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

## Examples

``` r
start <- ga_xy(0, 0)
end <- ga_xy(10, 0)

# one multipoint row, spaced at most 3 units apart
geoarrow::as_geoarrow_vctr(
  ga_points_along_line(start, end, 3, TRUE, "euclidean")
)
#> <geoarrow_vctr geoarrow.multipoint{list}[1]>
#> [1] <MULTIPOINT (0 0, 2.5 0, 5 0, 7.5 0, 10 0)>
```
