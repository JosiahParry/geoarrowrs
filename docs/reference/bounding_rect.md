# Compute the axis-aligned bounding rectangle of geometries

Returns the smallest axis-aligned rectangle that contains each geometry.

## Usage

``` r
bounding_rect(x)
```

## Arguments

- x:

  a GeoArrow geometry array

## Value

a GeoArrow rect array

## References

[BoundingRect](https://docs.rs/geo/latest/geo/algorithm/bounding_rect/trait.BoundingRect.html)

## See also

Other boundary:
[`concave_hull()`](https://josiahparry.github.io/geoarrowrs/reference/concave_hull.md),
[`convex_hull()`](https://josiahparry.github.io/geoarrowrs/reference/convex_hull.md),
[`extremes()`](https://josiahparry.github.io/geoarrowrs/reference/extremes.md),
[`minimum_rotated_rect()`](https://josiahparry.github.io/geoarrowrs/reference/minimum_rotated_rect.md)
