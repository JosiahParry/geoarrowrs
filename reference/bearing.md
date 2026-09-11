# Compute the bearing between pairs of points

Calculate the bearing in degrees from `origin` to `dest` for each pair
of points. Bearing is measured clockwise from north (0 degrees) to 360
degrees. These functions differ in the metric space used for the
calculation.

## Usage

``` r
ga_bearing_euclidean(origin, dest)

ga_bearing_haversine(origin, dest)

ga_bearing_geodesic(origin, dest)

ga_bearing_rhumb(origin, dest)
```

## Arguments

- origin:

  a GeoArrow point array of origin points

- dest:

  a GeoArrow point array; length 1 or the same length as `origin`

## Value

a double vector of bearing values in degrees

## References

[Bearing](https://docs.rs/geo/latest/geo/algorithm/line_measures/trait.Bearing.html),
[Euclidean](https://docs.rs/geo/latest/geo/algorithm/line_measures/metric_spaces/struct.Euclidean.html),
[Haversine](https://docs.rs/geo/latest/geo/algorithm/line_measures/metric_spaces/constant.Haversine.html),
[Geodesic](https://docs.rs/geo/latest/geo/algorithm/line_measures/metric_spaces/static.Geodesic.html),
[Rhumb](https://docs.rs/geo/latest/geo/algorithm/line_measures/metric_spaces/struct.Rhumb.html)

## Examples

``` r
# Raleigh to Charlotte, and Raleigh to Wilmington
origin <- ga_xy(c(-78.6382, -78.6382), c(35.7796, 35.7796))
dest <- ga_xy(c(-80.8431, -77.9447), c(35.2271, 34.2257))

# west-southwest, then south-southeast
as.vector(ga_bearing_haversine(origin, dest))
#> [1] 253.5337 159.7193
as.vector(ga_bearing_rhumb(origin, dest))
#> [1] 252.8913 159.9201
```
