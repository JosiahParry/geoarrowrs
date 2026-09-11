# Interpolate a point between two points

Returns the point located at `distance` along the path from `start` to
`end`. The metric determines how distance is measured.

Returns the point located at `ratio` of the way from `start` to `end`,
where 0.0 is the start and 1.0 is the end. The metric determines how the
interpolation is computed.

## Usage

``` r
ga_point_at_distance_between(start, end, distance, metric)

ga_point_at_ratio_between(start, end, ratio, metric)
```

## Arguments

- start:

  a GeoArrow point array of start points

- end:

  a GeoArrow point array of end points

- distance:

  a numeric vector of distances; length 1 or the same length as `start`

- metric:

  one of `"euclidean"`, `"haversine"`, `"geodesic"`, or `"rhumb"`

- ratio:

  a numeric vector of ratios between 0 and 1; length 1 or the same
  length as `start`

## Value

a GeoArrow point array

a GeoArrow point array

## References

[InterpolatePoint](https://docs.rs/geo/latest/geo/algorithm/line_measures/trait.InterpolatePoint.html)

## See also

Other interpolate:
[`ga_interpolate_point()`](https://josiahparry.github.io/geoarrowrs/reference/ga_interpolate_point.md),
[`ga_points_along_line()`](https://josiahparry.github.io/geoarrowrs/reference/ga_points_along_line.md)

## Examples

``` r
start <- ga_xy(c(0, 0), c(0, 0))
end <- ga_xy(c(10, 10), c(0, 0))

# three units along, then a third of the way along
geoarrow::as_geoarrow_vctr(
  ga_point_at_distance_between(start, end, c(3, 3), "euclidean")
)
#> <geoarrow_vctr geoarrow.point{struct}[2]>
#> [1] <POINT (3 0)> <POINT (3 0)>
geoarrow::as_geoarrow_vctr(
  ga_point_at_ratio_between(start, end, c(1 / 3, 1 / 3), "euclidean")
)
#> <geoarrow_vctr geoarrow.point{struct}[2]>
#> [1] <POINT (3.3333333 0)> <POINT (3.3333333 0)>
```
