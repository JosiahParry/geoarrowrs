# Compute the convex hull of geometries

Returns the smallest convex polygon that contains each geometry.

## Usage

``` r
ga_convex_hull(x)
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
[`ga_bounding_rect()`](https://josiahparry.github.io/geoarrowrs/reference/ga_bounding_rect.md),
[`ga_concave_hull()`](https://josiahparry.github.io/geoarrowrs/reference/ga_concave_hull.md),
[`ga_extremes()`](https://josiahparry.github.io/geoarrowrs/reference/ga_extremes.md),
[`ga_minimum_rotated_rect()`](https://josiahparry.github.io/geoarrowrs/reference/ga_minimum_rotated_rect.md)
