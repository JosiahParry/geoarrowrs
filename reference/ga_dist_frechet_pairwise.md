# Pairwise Frechet distance

The Frechet distance measures the similarity between two curves by
considering the location and ordering of points along each curve. Uses
the Euclidean metric.

## Usage

``` r
ga_dist_frechet_pairwise(origin, dest)
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
[`ga_dist_euclidean_pairwise()`](https://josiahparry.github.io/geoarrowrs/reference/dist_pairwise.md),
[`ga_dist_hausdorff_pairwise()`](https://josiahparry.github.io/geoarrowrs/reference/ga_dist_hausdorff_pairwise.md),
[`ga_dist_vincenty_pairwise()`](https://josiahparry.github.io/geoarrowrs/reference/ga_dist_vincenty_pairwise.md)

## Examples

``` r
# a straight route against one that detours north
direct <- geoarrow::as_geoarrow_array(sf::st_sfc(
  sf::st_linestring(rbind(c(0, 0), c(2, 0), c(4, 0)))
))
detour <- geoarrow::as_geoarrow_array(sf::st_sfc(
  sf::st_linestring(rbind(c(0, 0), c(2, 1), c(4, 0)))
))

as.vector(ga_dist_frechet_pairwise(direct, detour))
#> [1] 1
```
