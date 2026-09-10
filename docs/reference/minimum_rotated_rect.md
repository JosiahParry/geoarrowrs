# Compute the minimum rotated bounding rectangle of geometries

Returns the smallest rectangle of arbitrary rotation that contains each
geometry, as a polygon array.

## Usage

``` r
minimum_rotated_rect(x)
```

## Arguments

- x:

  a GeoArrow geometry array

## Value

a GeoArrow polygon array

## References

[MinimumRotatedRect](https://docs.rs/geo/latest/geo/algorithm/minimum_rotated_rect/trait.MinimumRotatedRect.html)

## See also

Other boundary:
[`bounding_rect()`](https://josiahparry.github.io/geoarrowrs/reference/bounding_rect.md),
[`concave_hull()`](https://josiahparry.github.io/geoarrowrs/reference/concave_hull.md),
[`convex_hull()`](https://josiahparry.github.io/geoarrowrs/reference/convex_hull.md),
[`extremes()`](https://josiahparry.github.io/geoarrowrs/reference/extremes.md)
