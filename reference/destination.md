# Compute a destination point from an origin, bearing, and distance

Returns the point reached by travelling `distance` from `origin` along
`bearing`. Each function uses a different metric space: `dest_euclidean`
treats coordinates as planar, `dest_haversine` assumes a sphere,
`dest_geodesic` an ellipsoid, and `dest_rhumb` follows a line of
constant bearing.

## Usage

``` r
ga_dest_rhumb(origin, bearing, distance)

ga_dest_euclidean(origin, bearing, distance)

ga_dest_haversine(origin, bearing, distance)

ga_dest_geodesic(origin, bearing, distance)
```

## Arguments

- origin:

  a GeoArrow point array

- bearing:

  a numeric vector of bearings in degrees; length 1 or the same length
  as `origin`

- distance:

  a numeric vector of distances; length 1 or the same length as `origin`

## Value

a GeoArrow point array

## References

[Destination](https://docs.rs/geo/latest/geo/algorithm/line_measures/trait.Destination.html)
