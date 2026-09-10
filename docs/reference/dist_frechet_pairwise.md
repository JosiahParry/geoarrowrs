# Compute the pairwise Frechet distance between linestrings

The Frechet distance measures the similarity between two curves by
considering the location and ordering of points along each curve. Uses
the Euclidean metric.

## Usage

``` r
dist_frechet_pairwise(origin, dest)
```

## Arguments

- origin:

  a GeoArrow linestring array

- dest:

  a GeoArrow linestring array

## Value

a double vector of Frechet distance values

## References

[FrechetDistance](https://docs.rs/geo/latest/geo/algorithm/line_measures/trait.FrechetDistance.html)

## See also

Other distance:
[`dist_euclidean_pairwise()`](https://josiahparry.github.io/geoarrowrs/reference/dist_pairwise.md),
[`dist_hausdorff_pairwise()`](https://josiahparry.github.io/geoarrowrs/reference/dist_hausdorff_pairwise.md),
[`dist_vincenty_pairwise()`](https://josiahparry.github.io/geoarrowrs/reference/dist_vincenty_pairwise.md)
