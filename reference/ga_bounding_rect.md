# Axis-aligned bounding rectangle

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

## Examples

``` r
fp <- system.file("shape/nc.shp", package = "sf")
nc <- as.data.frame(read_shapefile(fp))

head(geoarrow::as_geoarrow_vctr(ga_bounding_rect(nc$geometry)), 3)
#> <geoarrow_vctr geoarrow.box{struct}[3]>
#> [1] <POLYGON ((-81.7410736 36.2343559, -81.2398911 36.2343559, -81.2398911 >
#> [2] <POLYGON ((-81.3475418 36.3653641, -80.9034424 36.3653641, -80.9034424 >
#> [3] <POLYGON ((-80.9657745 36.2338829, -80.4353104 36.2338829, -80.4353104 >
```
