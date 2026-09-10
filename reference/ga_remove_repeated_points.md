# Remove repeated consecutive points from geometries

Removes consecutive duplicate coordinates from each geometry. Accepts
linestrings, multilinestrings, polygons, and multipolygons.

## Usage

``` r
ga_remove_repeated_points(geometry)
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
[`ga_buffer()`](https://josiahparry.github.io/geoarrowrs/reference/ga_buffer.md),
[`ga_centroid()`](https://josiahparry.github.io/geoarrowrs/reference/ga_centroid.md),
[`ga_chaikin_smoothing()`](https://josiahparry.github.io/geoarrowrs/reference/ga_chaikin_smoothing.md),
[`ga_line_segmentize()`](https://josiahparry.github.io/geoarrowrs/reference/ga_line_segmentize.md)
