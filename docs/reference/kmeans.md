# Assign points to a fixed number of clusters

Splits each geometry's points into `k` clusters by nearest centre.
Returns one list of labels per input geometry.

## Usage

``` r
kmeans(geometry, k, seed = NULL)
```

## Arguments

- geometry:

  a GeoArrow multipoint array

- k:

  how many clusters to produce; length 1 or the same length as
  `geometry`

- seed:

  a seed for reproducible starts, or `NULL` to vary each run

## Value

a list array of integer labels, one list per input geometry

## Details

k-means always produces exactly `k` clusters and no noise, so every
point gets a label. It favours round, similarly sized clusters; use
[`dbscan()`](https://josiahparry.github.io/geoarrowrs/reference/dbscan.md)
when the shapes are irregular or the count is unknown.

The algorithm starts from a random seed, so results vary between runs
unless `seed` is given. A row with fewer than `k` points cannot be split
and comes back null, as does a row that is not point based or is null.

## References

[KMeans](https://docs.rs/geo/latest/geo/algorithm/kmeans/trait.KMeans.html)

## See also

Other cluster:
[`dbscan()`](https://josiahparry.github.io/geoarrowrs/reference/dbscan.md),
[`outlier_scores()`](https://josiahparry.github.io/geoarrowrs/reference/outlier_scores.md)

## Examples

``` r
pts <- sf::st_multipoint(cbind(
  c(0, 0.1, 0.2, 5, 5.1, 5.2),
  c(0, 0.1, 0.2, 5, 5.1, 5.2)
))
g <- geoarrow::as_geoarrow_array(sf::st_sfc(pts))

nanoarrow::convert_array(kmeans(g, k = 2, seed = 1))
#> <list_of<integer>[1]>
#> [[1]]
#> [1] 1 1 1 2 2 2
#> 
```
