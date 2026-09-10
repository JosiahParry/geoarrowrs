# Compute the centroid of geometries

Returns the centroid point of each geometry. Returns `NA` for empty
geometries.

## Usage

``` r
ga_centroid(x)
```

## Arguments

- x:

  a GeoArrow geometry array

## Value

a GeoArrow point array

## References

[Centroid](https://docs.rs/geo/latest/geo/algorithm/centroid/trait.Centroid.html)

## See also

Other misc:
[`ga_buffer()`](https://josiahparry.github.io/geoarrowrs/reference/ga_buffer.md),
[`ga_chaikin_smoothing()`](https://josiahparry.github.io/geoarrowrs/reference/ga_chaikin_smoothing.md),
[`ga_line_segmentize()`](https://josiahparry.github.io/geoarrowrs/reference/ga_line_segmentize.md),
[`ga_remove_repeated_points()`](https://josiahparry.github.io/geoarrowrs/reference/ga_remove_repeated_points.md)
