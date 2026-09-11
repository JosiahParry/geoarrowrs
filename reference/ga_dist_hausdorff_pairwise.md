# Pairwise Hausdorff distance

The Hausdorff distance measures how far two geometries are from each
other by taking the maximum of all minimum distances between points on
the two shapes.

## Usage

``` r
ga_dist_hausdorff_pairwise(origin, dest)
```

## Arguments

- origin:

  a GeoArrow geometry array

- dest:

  a GeoArrow geometry array

## Value

a double vector of Hausdorff distance values

## References

[HausdorffDistance](https://docs.rs/geo/latest/geo/algorithm/hausdorff_distance/trait.HausdorffDistance.html)

## See also

Other distance:
[`ga_dist_euclidean_pairwise()`](https://josiahparry.github.io/geoarrowrs/reference/dist_pairwise.md),
[`ga_dist_frechet_pairwise()`](https://josiahparry.github.io/geoarrowrs/reference/ga_dist_frechet_pairwise.md),
[`ga_dist_vincenty_pairwise()`](https://josiahparry.github.io/geoarrowrs/reference/ga_dist_vincenty_pairwise.md)

## Examples

``` r
# how far apart are neighbouring counties at their worst?
fp <- system.file("shape/nc.shp", package = "sf")
nc <- as.data.frame(read_shapefile(fp))

as.vector(ga_dist_hausdorff_pairwise(nc$geometry[1:5], nc$geometry[2:6]))
#> [1] 0.4197870 0.5225070 4.6732471 1.5716317 0.7253396
```
