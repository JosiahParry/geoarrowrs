# Add intermediate points to geometries so no segment exceeds a maximum length

Densifies linestrings, multilinestrings, polygons, and multipolygons by
inserting additional points along each segment until no segment exceeds
`max_segment_length`. The metric determines how segment length is
measured.

## Usage

``` r
densify(geometry, max_segment_length, metric)
```

## Arguments

- geometry:

  a GeoArrow linestring, multilinestring, polygon, or multipolygon array

- max_segment_length:

  the maximum segment length; either length 1 or the same length as
  `geometry`

- metric:

  one of `"euclidean"`, `"haversine"`, `"geodesic"`, or `"rhumb"`

## Value

a GeoArrow array of the same geometry type as the input

## References

[Densifiable](https://docs.rs/geo/latest/geo/algorithm/line_measures/trait.Densifiable.html)
