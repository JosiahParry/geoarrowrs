# Compute the convex hull of geometries

Returns the smallest convex polygon that contains each geometry.

## Usage

``` r
convex_hull(x)
```

## Arguments

- x:

  a GeoArrow geometry array

## Value

a GeoArrow polygon array

## References

[ConvexHull](https://docs.rs/geo/latest/geo/algorithm/convex_hull/trait.ConvexHull.html)

## See also

Other boundary:
[`bounding_rect()`](https://josiahparry.github.io/geoarrowrs/reference/bounding_rect.md),
[`concave_hull()`](https://josiahparry.github.io/geoarrowrs/reference/concave_hull.md),
[`extremes()`](https://josiahparry.github.io/geoarrowrs/reference/extremes.md),
[`minimum_rotated_rect()`](https://josiahparry.github.io/geoarrowrs/reference/minimum_rotated_rect.md)
