# Compute pairwise distances between points

Calculate the distance between each pair of points in `origin` and
`dest`. These functions differ in the metric space used for the
calculation.

## Usage

``` r
dist_euclidean_pairwise(origin, dest)

dist_haversine_pairwise(origin, dest)

dist_geodesic_pairwise(origin, dest)

dist_rhumb_pairwise(origin, dest)
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
[Euclidean](https://docs.rs/geo/latest/geo/algorithm/line_measures/metric_spaces/struct.Euclidean.html)

[Distance](https://docs.rs/geo/latest/geo/algorithm/line_measures/trait.Distance.html),
[Haversine](https://docs.rs/geo/latest/geo/algorithm/line_measures/metric_spaces/constant.Haversine.html)

[Distance](https://docs.rs/geo/latest/geo/algorithm/line_measures/trait.Distance.html),
[Geodesic](https://docs.rs/geo/latest/geo/algorithm/line_measures/metric_spaces/static.Geodesic.html)

[Distance](https://docs.rs/geo/latest/geo/algorithm/line_measures/trait.Distance.html),
[Rhumb](https://docs.rs/geo/latest/geo/algorithm/line_measures/metric_spaces/struct.Rhumb.html)

## See also

Other distance:
[`dist_frechet_pairwise()`](https://josiahparry.github.io/geoarrowrs/reference/dist_frechet_pairwise.md),
[`dist_hausdorff_pairwise()`](https://josiahparry.github.io/geoarrowrs/reference/dist_hausdorff_pairwise.md),
[`dist_vincenty_pairwise()`](https://josiahparry.github.io/geoarrowrs/reference/dist_vincenty_pairwise.md)
