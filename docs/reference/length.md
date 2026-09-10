# Compute the length of linestrings

These functions calculate the total length of each linestring using
different metric spaces. Use the geodesic or haversine variants for
geographic coordinates, and euclidean for projected coordinates.

Calculates the geodesic length of each linestring or multilinestring
using the Vincenty formula on an ellipsoidal model of the earth. Returns
`NA` if the algorithm fails to converge. Results are in meters.

## Usage

``` r
length_euclidean(x)

length_haversine(x)

length_geodesic(x)

length_rhumb(x)

length_vincenty(x)
```

## Arguments

- x:

  a GeoArrow linestring or multilinestring array

## Value

a double vector of length values

a double vector of length values in meters

## References

[Length](https://docs.rs/geo/latest/geo/algorithm/line_measures/trait.Length.html),
[Euclidean](https://docs.rs/geo/latest/geo/algorithm/line_measures/metric_spaces/struct.Euclidean.html)

[Length](https://docs.rs/geo/latest/geo/algorithm/line_measures/trait.Length.html),
[Haversine](https://docs.rs/geo/latest/geo/algorithm/line_measures/metric_spaces/constant.Haversine.html)

[Length](https://docs.rs/geo/latest/geo/algorithm/line_measures/trait.Length.html),
[Geodesic](https://docs.rs/geo/latest/geo/algorithm/line_measures/metric_spaces/static.Geodesic.html)

[Length](https://docs.rs/geo/latest/geo/algorithm/line_measures/trait.Length.html),
[Rhumb](https://docs.rs/geo/latest/geo/algorithm/line_measures/metric_spaces/struct.Rhumb.html)

[VincentyLength](https://docs.rs/geo/latest/geo/algorithm/vincenty_length/trait.VincentyLength.html)
