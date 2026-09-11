# Minimum rotated bounding rectangle

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

## Examples

``` r
fp <- system.file("shape/nc.shp", package = "sf")
nc <- as.data.frame(read_shapefile(fp))

# free to rotate, so it hugs the county tighter than the envelope
rot <- ga_minimum_rotated_rect(nc$geometry)
head(as.vector(ga_unsigned_area(rot)), 3)
#> [1] 0.17296583 0.09121554 0.17446307
head(as.vector(ga_unsigned_area(ga_bounding_rect(nc$geometry))), 3)
#> [1] 0.17806679 0.09215083 0.17575835
```
