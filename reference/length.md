# Compute the length of linestrings

These functions calculate the total length of each linestring using
different metric spaces. Use the geodesic or haversine variants for
geographic coordinates, and euclidean for projected coordinates.

Calculates the geodesic length of each linestring or multilinestring
using the Vincenty formula on an ellipsoidal model of the earth. Returns
`NA` if the algorithm fails to converge. Results are in meters.

## Usage

``` r
ga_length_euclidean(x)

ga_length_haversine(x)

ga_length_geodesic(x)

ga_length_rhumb(x)

ga_length_vincenty(x)
```

## Arguments

- x:

  a GeoArrow linestring or multilinestring array

## Value

a double vector of length values

a double vector of length values in meters

## References

[Length](https://docs.rs/geo/latest/geo/algorithm/line_measures/trait.Length.html),
[Euclidean](https://docs.rs/geo/latest/geo/algorithm/line_measures/metric_spaces/struct.Euclidean.html),
[Haversine](https://docs.rs/geo/latest/geo/algorithm/line_measures/metric_spaces/constant.Haversine.html),
[Geodesic](https://docs.rs/geo/latest/geo/algorithm/line_measures/metric_spaces/static.Geodesic.html),
[Rhumb](https://docs.rs/geo/latest/geo/algorithm/line_measures/metric_spaces/struct.Rhumb.html)

[VincentyLength](https://docs.rs/geo/latest/geo/algorithm/vincenty_length/trait.VincentyLength.html)

## Examples

``` r
# Raleigh to Charlotte, and Raleigh to Wilmington
trips <- geoarrow::as_geoarrow_array(sf::st_sfc(
  sf::st_linestring(rbind(c(-78.6382, 35.7796), c(-80.8431, 35.2271))),
  sf::st_linestring(rbind(c(-78.6382, 35.7796), c(-77.9447, 34.2257)))
))

# degrees, then meters
as.vector(ga_length_euclidean(trips))
#> [1] 2.273068 1.701631
as.vector(ga_length_geodesic(trips))
#> [1] 209216.6 183645.3
```
