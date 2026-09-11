# Compute pairwise distances between points

Calculate the distance between each pair of points in `origin` and
`dest`. These functions differ in the metric space used for the
calculation.

## Usage

``` r
ga_dist_euclidean_pairwise(origin, dest)

ga_dist_haversine_pairwise(origin, dest)

ga_dist_geodesic_pairwise(origin, dest)

ga_dist_rhumb_pairwise(origin, dest)
```

## Arguments

- origin:

  a GeoArrow point array of origin points

- dest:

  a GeoArrow point array of destination points

## Value

a double vector of distance values

## References

[Distance](https://docs.rs/geo/latest/geo/algorithm/line_measures/trait.Distance.html),
[Euclidean](https://docs.rs/geo/latest/geo/algorithm/line_measures/metric_spaces/struct.Euclidean.html),
[Haversine](https://docs.rs/geo/latest/geo/algorithm/line_measures/metric_spaces/constant.Haversine.html),
[Geodesic](https://docs.rs/geo/latest/geo/algorithm/line_measures/metric_spaces/static.Geodesic.html),
[Rhumb](https://docs.rs/geo/latest/geo/algorithm/line_measures/metric_spaces/struct.Rhumb.html)

## See also

Other distance:
[`ga_dist_frechet_pairwise()`](https://josiahparry.github.io/geoarrowrs/reference/ga_dist_frechet_pairwise.md),
[`ga_dist_hausdorff_pairwise()`](https://josiahparry.github.io/geoarrowrs/reference/ga_dist_hausdorff_pairwise.md),
[`ga_dist_vincenty_pairwise()`](https://josiahparry.github.io/geoarrowrs/reference/ga_dist_vincenty_pairwise.md)

## Examples

``` r
# Raleigh to Charlotte, and Raleigh to Wilmington
origin <- ga_xy(c(-78.6382, -78.6382), c(35.7796, 35.7796))
dest <- ga_xy(c(-80.8431, -77.9447), c(35.2271, 34.2257))

# degrees, then meters
as.vector(ga_dist_euclidean_pairwise(origin, dest))
#> [1] 2.273068 1.701631
as.vector(ga_dist_geodesic_pairwise(origin, dest))
#> [1] 209216.6 183645.3
```
