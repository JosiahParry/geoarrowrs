# Assign points to clusters by density

Labels every point in the array with a cluster number, or `NA` when the
point is noise. One label per row, in the order the points were given.

## Usage

``` r
ga_dbscan(geometry, eps, min_points)
```

## Arguments

- geometry:

  a GeoArrow point array

- eps:

  how close two points must be to be neighbours

- min_points:

  how many neighbours a point needs to seed a cluster

## Value

an integer array of cluster labels, the same length as `geometry`

## Details

DBSCAN grows a cluster from any point with at least `min_points`
neighbours within `eps`. Points reachable from that core join the
cluster, and points that never become reachable are noise. Unlike
k-means it finds clusters of any shape and does not need the count up
front.

Clustering is over the whole array, not within each row, so `eps` and
`min_points` are single values rather than one per row. Cluster numbers
start at 1 and mean nothing beyond grouping. A row that is not a single
point, or is null, takes no part in the clustering and comes back null.

## References

[Dbscan](https://docs.rs/geo/latest/geo/algorithm/dbscan/trait.Dbscan.html)

## See also

Other cluster:
[`ga_kmeans()`](https://josiahparry.github.io/geoarrowrs/reference/ga_kmeans.md),
[`ga_outlier_scores()`](https://josiahparry.github.io/geoarrowrs/reference/ga_outlier_scores.md)

## Examples

``` r
g <- ga_xy(
  c(0, 0.1, 0.2, 5, 5.1, 5.2),
  c(0, 0.1, 0.2, 5, 5.1, 5.2)
)

as.vector(ga_dbscan(g, eps = 1, min_points = 2))
#> [1] 1 1 1 2 2 2
```
