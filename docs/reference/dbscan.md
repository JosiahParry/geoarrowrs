# Assign points to clusters by density

Labels each point of a geometry with a cluster number, or `NA` when the
point is noise. Returns one list of labels per input geometry.

## Usage

``` r
dbscan(geometry, eps, min_points)
```

## Arguments

- geometry:

  a GeoArrow multipoint array

- eps:

  how close two points must be to be neighbours; length 1 or the same
  length as `geometry`

- min_points:

  how many neighbours a point needs to seed a cluster; length 1 or the
  same length as `geometry`

## Value

a list array of integer labels, one list per input geometry

## Details

DBSCAN grows a cluster from any point with at least `min_points`
neighbours within `eps`. Points reachable from that core join the
cluster, and points that never become reachable are noise. Unlike
k-means it finds clusters of any shape and does not need the count up
front.

Cluster numbers start at 1 and mean nothing beyond grouping. Both `eps`
and `min_points` are recycled against `geometry`. A row that is not
point based, or is null, comes back null.

## References

[Dbscan](https://docs.rs/geo/latest/geo/algorithm/dbscan/trait.Dbscan.html)

## See also

Other cluster:
[`kmeans()`](https://josiahparry.github.io/geoarrowrs/reference/kmeans.md),
[`outlier_scores()`](https://josiahparry.github.io/geoarrowrs/reference/outlier_scores.md)

## Examples

``` r
pts <- sf::st_multipoint(cbind(
  c(0, 0.1, 0.2, 5, 5.1, 5.2),
  c(0, 0.1, 0.2, 5, 5.1, 5.2)
))
g <- geoarrow::as_geoarrow_array(sf::st_sfc(pts))

nanoarrow::convert_array(dbscan(g, eps = 1, min_points = 2))
#> <list_of<integer>[1]>
#> [[1]]
#> [1] 1 1 1 2 2 2
#> 
```
