# Compute the bearing between pairs of points

Calculate the bearing in degrees from `origin` to `dest` for each pair
of points. Bearing is measured clockwise from north (0 degrees) to 360
degrees. These functions differ in the metric space used for the
calculation.

## Usage

``` r
bearing_euclidean(origin, dest)

bearing_haversine(origin, dest)

bearing_geodesic(origin, dest)

bearing_rhumb(origin, dest)
```

## Arguments

- origin:

  a GeoArrow point array of origin points

- dest:

  a GeoArrow point array of destination points

## Value

a double vector of bearing values in degrees

## References

[Bearing](https://docs.rs/geo/latest/geo/algorithm/line_measures/trait.Bearing.html),
[Euclidean](https://docs.rs/geo/latest/geo/algorithm/line_measures/metric_spaces/struct.Euclidean.html)

[Bearing](https://docs.rs/geo/latest/geo/algorithm/line_measures/trait.Bearing.html),
[Haversine](https://docs.rs/geo/latest/geo/algorithm/line_measures/metric_spaces/constant.Haversine.html)

[Bearing](https://docs.rs/geo/latest/geo/algorithm/line_measures/trait.Bearing.html),
[Geodesic](https://docs.rs/geo/latest/geo/algorithm/line_measures/metric_spaces/static.Geodesic.html)

[Bearing](https://docs.rs/geo/latest/geo/algorithm/line_measures/trait.Bearing.html),
[Rhumb](https://docs.rs/geo/latest/geo/algorithm/line_measures/metric_spaces/struct.Rhumb.html)
