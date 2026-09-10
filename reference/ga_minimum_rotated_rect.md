# Compute the minimum rotated bounding rectangle of geometries

Returns the smallest rectangle of arbitrary rotation that contains each
geometry, as a polygon array.

## Usage

``` r
ga_minimum_rotated_rect(x)
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
[`ga_bounding_rect()`](https://josiahparry.github.io/geoarrowrs/reference/ga_bounding_rect.md),
[`ga_concave_hull()`](https://josiahparry.github.io/geoarrowrs/reference/ga_concave_hull.md),
[`ga_convex_hull()`](https://josiahparry.github.io/geoarrowrs/reference/ga_convex_hull.md),
[`ga_extremes()`](https://josiahparry.github.io/geoarrowrs/reference/ga_extremes.md)
