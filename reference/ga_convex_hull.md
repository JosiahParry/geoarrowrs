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

## Examples

``` r
fp <- system.file("shape/nc.shp", package = "sf")
nc <- as.data.frame(read_shapefile(fp))

# the hull fills in every concavity, so it is never smaller
head(as.vector(ga_unsigned_area(ga_convex_hull(nc$geometry))), 3)
#> [1] 0.12484563 0.07437819 0.15586877
head(as.vector(ga_unsigned_area(nc$geometry)), 3)
#> [1] 0.11428350 0.06139976 0.14301628
```
