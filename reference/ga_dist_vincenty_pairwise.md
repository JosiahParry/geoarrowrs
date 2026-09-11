# Pairwise Vincenty distance

The Vincenty formula computes the geodesic distance between two points
on an ellipsoidal model of the earth. Returns `NA` if the algorithm
fails to converge.

## Usage

``` r
ga_dist_vincenty_pairwise(origin, dest)
```

## Arguments

- origin:

  a GeoArrow point array of origin points

- dest:

  a GeoArrow point array; length 1 or the same length as `origin`

## Value

a double vector of distance values in meters

## References

[VincentyDistance](https://docs.rs/geo/latest/geo/algorithm/vincenty_distance/trait.VincentyDistance.html)

## See also

Other distance:
[`ga_cross_distance()`](https://josiahparry.github.io/geoarrowrs/reference/ga_cross_distance.md),
[`ga_dist_euclidean_pairwise()`](https://josiahparry.github.io/geoarrowrs/reference/dist_pairwise.md),
[`ga_dist_frechet_pairwise()`](https://josiahparry.github.io/geoarrowrs/reference/ga_dist_frechet_pairwise.md),
[`ga_dist_hausdorff_pairwise()`](https://josiahparry.github.io/geoarrowrs/reference/ga_dist_hausdorff_pairwise.md)

## Examples

``` r
# Raleigh to Charlotte, and Raleigh to Wilmington
origin <- ga_xy(c(-78.6382, -78.6382), c(35.7796, 35.7796))
dest <- ga_xy(c(-80.8431, -77.9447), c(35.2271, 34.2257))

as.vector(ga_dist_vincenty_pairwise(origin, dest))
#> [1] 209216.6 183645.3
```
