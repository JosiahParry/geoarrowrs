# Compute the pairwise Vincenty distance between points

The Vincenty formula computes the geodesic distance between two points
on an ellipsoidal model of the earth. Returns `NA` if the algorithm
fails to converge.

## Usage

``` r
dist_vincenty_pairwise(origin, dest)
```

## Arguments

- origin:

  a GeoArrow point array of origin points

- dest:

  a GeoArrow point array of destination points

## Value

a double vector of distance values in meters

## References

[VincentyDistance](https://docs.rs/geo/latest/geo/algorithm/vincenty_distance/trait.VincentyDistance.html)

## See also

Other distance:
[`dist_euclidean_pairwise()`](https://josiahparry.github.io/geoarrowrs/reference/dist_pairwise.md),
[`dist_frechet_pairwise()`](https://josiahparry.github.io/geoarrowrs/reference/dist_frechet_pairwise.md),
[`dist_hausdorff_pairwise()`](https://josiahparry.github.io/geoarrowrs/reference/dist_hausdorff_pairwise.md)
