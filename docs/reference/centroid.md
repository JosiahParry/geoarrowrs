# Compute the centroid of geometries

Returns the centroid point of each geometry. Returns `NA` for empty
geometries.

## Usage

``` r
centroid(x)
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
[`buffer()`](https://josiahparry.github.io/geoarrowrs/reference/buffer.md),
[`chaikin_smoothing()`](https://josiahparry.github.io/geoarrowrs/reference/chaikin_smoothing.md),
[`line_segmentize()`](https://josiahparry.github.io/geoarrowrs/reference/line_segmentize.md),
[`remove_repeated_points()`](https://josiahparry.github.io/geoarrowrs/reference/remove_repeated_points.md)
