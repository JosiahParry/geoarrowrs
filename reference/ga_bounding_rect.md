# Compute the axis-aligned bounding rectangle of geometries

Returns the smallest axis-aligned rectangle that contains each geometry.

## Usage

``` r
ga_bounding_rect(x)
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
[`ga_concave_hull()`](https://josiahparry.github.io/geoarrowrs/reference/ga_concave_hull.md),
[`ga_convex_hull()`](https://josiahparry.github.io/geoarrowrs/reference/ga_convex_hull.md),
[`ga_extremes()`](https://josiahparry.github.io/geoarrowrs/reference/ga_extremes.md),
[`ga_minimum_rotated_rect()`](https://josiahparry.github.io/geoarrowrs/reference/ga_minimum_rotated_rect.md)
