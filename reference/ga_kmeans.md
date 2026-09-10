# Assign points to a fixed number of clusters

Splits the points in the array into `k` clusters by nearest centre. One
label per row, in the order the points were given.

## Usage

``` r
ga_kmeans(geometry, k, seed = NULL)
```

## Arguments

- geometry:

  a GeoArrow point array

- k:

  how many clusters to produce

- seed:

  a seed for reproducible starts, or `NULL` to vary each run

## Value

an integer array of cluster labels, the same length as `geometry`

## Details

k-means always produces exactly `k` clusters and no noise, so every
point gets a label. It favours round, similarly sized clusters; use
[`ga_dbscan()`](https://josiahparry.github.io/geoarrowrs/reference/ga_dbscan.md)
when the shapes are irregular or the count is unknown.

Clustering is over the whole array, not within each row, so `k` is a
single value rather than one per row. The algorithm starts from a random
seed, so results vary between runs unless `seed` is given. A row that is
not a single point, or is null, takes no part in the clustering and
comes back null.

## References

[KMeans](https://docs.rs/geo/latest/geo/algorithm/kmeans/trait.KMeans.html)

## See also

Other cluster:
[`ga_dbscan()`](https://josiahparry.github.io/geoarrowrs/reference/ga_dbscan.md),
[`ga_outlier_scores()`](https://josiahparry.github.io/geoarrowrs/reference/ga_outlier_scores.md)

## Examples

``` r
g <- ga_xy(
  c(0, 0.1, 0.2, 5, 5.1, 5.2),
  c(0, 0.1, 0.2, 5, 5.1, 5.2)
)

as.vector(ga_kmeans(g, k = 2, seed = 1))
#> [1] 1 1 1 2 2 2
```
