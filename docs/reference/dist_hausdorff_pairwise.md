# Compute the pairwise Hausdorff distance between geometries

The Hausdorff distance measures how far two geometries are from each
other by taking the maximum of all minimum distances between points on
the two shapes.

## Usage

``` r
dist_hausdorff_pairwise(origin, dest)
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
[`dist_euclidean_pairwise()`](https://josiahparry.github.io/geoarrowrs/reference/dist_pairwise.md),
[`dist_frechet_pairwise()`](https://josiahparry.github.io/geoarrowrs/reference/dist_frechet_pairwise.md),
[`dist_vincenty_pairwise()`](https://josiahparry.github.io/geoarrowrs/reference/dist_vincenty_pairwise.md)
