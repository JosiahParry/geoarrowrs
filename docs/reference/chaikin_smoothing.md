# Smooth geometries using the Chaikin algorithm

Applies Chaikin's corner-cutting algorithm for the given number of
iterations to produce smoother curves. Accepts linestrings,
multilinestrings, polygons, and multipolygons.

## Usage

``` r
chaikin_smoothing(geometry, n_iterations)
```

## Arguments

- geometry:

  a GeoArrow linestring, multilinestring, polygon, or multipolygon array

- n_iterations:

  the number of smoothing iterations to apply; must be greater than 0

## Value

a GeoArrow array of the same geometry type as the input

## References

[ChaikinSmoothing](https://docs.rs/geo/latest/geo/algorithm/chaikin_smoothing/trait.ChaikinSmoothing.html)

## See also

Other misc:
[`buffer()`](https://josiahparry.github.io/geoarrowrs/reference/buffer.md),
[`centroid()`](https://josiahparry.github.io/geoarrowrs/reference/centroid.md),
[`line_segmentize()`](https://josiahparry.github.io/geoarrowrs/reference/line_segmentize.md),
[`remove_repeated_points()`](https://josiahparry.github.io/geoarrowrs/reference/remove_repeated_points.md)
