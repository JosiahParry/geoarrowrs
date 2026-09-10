# Remove repeated consecutive points from geometries

Removes consecutive duplicate coordinates from each geometry. Accepts
linestrings, multilinestrings, polygons, and multipolygons.

## Usage

``` r
remove_repeated_points(geometry)
```

## Arguments

- geometry:

  a GeoArrow linestring, multilinestring, polygon, or multipolygon array

## Value

a GeoArrow array of the same geometry type as the input

## References

[RemoveRepeatedPoints](https://docs.rs/geo/latest/geo/algorithm/remove_repeated_points/trait.RemoveRepeatedPoints.html)

## See also

Other misc:
[`buffer()`](https://josiahparry.github.io/geoarrowrs/reference/buffer.md),
[`centroid()`](https://josiahparry.github.io/geoarrowrs/reference/centroid.md),
[`chaikin_smoothing()`](https://josiahparry.github.io/geoarrowrs/reference/chaikin_smoothing.md),
[`line_segmentize()`](https://josiahparry.github.io/geoarrowrs/reference/line_segmentize.md)
