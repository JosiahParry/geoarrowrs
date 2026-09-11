# Interpolate a point along a linestring

Returns the point at a given ratio or distance along each linestring,
measured from either the start or end. The metric determines how
distance is calculated.

## Usage

``` r
ga_interpolate_point(line, value, metric, measure, from)
```

## Arguments

- line:

  a GeoArrow linestring array

- value:

  a numeric vector of ratio or distance values; length 1 or the same
  length as `line`

- metric:

  one of `"euclidean"`, `"haversine"`, `"geodesic"`, or `"rhumb"`

- measure:

  one of `"ratio"` or `"distance"`

- from:

  one of `"start"` or `"end"`

## Value

a GeoArrow point array

## References

[InterpolateLine](https://docs.rs/geo/latest/geo/algorithm/line_measures/trait.InterpolateLine.html)

## See also

Other interpolate:
[`ga_point_at_distance_between()`](https://josiahparry.github.io/geoarrowrs/reference/interpolate_between.md),
[`ga_points_along_line()`](https://josiahparry.github.io/geoarrowrs/reference/ga_points_along_line.md)

## Examples

``` r
line <- geoarrow::as_geoarrow_array(sf::st_sfc(
  sf::st_linestring(rbind(c(0, 0), c(10, 0)))
))

# halfway from the start, then two units in from the end
geoarrow::as_geoarrow_vctr(
  ga_interpolate_point(line, 0.5, "euclidean", "ratio", "start")
)
#> <geoarrow_vctr geoarrow.point{struct}[1]>
#> [1] <POINT (5 0)>
geoarrow::as_geoarrow_vctr(
  ga_interpolate_point(line, 2, "euclidean", "distance", "end")
)
#> <geoarrow_vctr geoarrow.point{struct}[1]>
#> [1] <POINT (8 0)>
```
