# Join two data frames on nearest neighbours

Attaches the columns of the `k` rows of `y` nearest each row of `x`, the
way
[`ga_join()`](https://josiahparry.github.io/geoarrowrs/reference/ga_join.md)
attaches the ones that relate to it. `ga_knn_join()` also adds the
distance between each pair as a column.

## Usage

``` r
ga_knn_join(
  x,
  y,
  k = 1,
  ...,
  max_distance = NULL,
  distance = "distance",
  suffix = c("_x", "_y"),
  left = TRUE
)
```

## Arguments

- x, y:

  data frames, each with one GeoArrow geometry column

- k:

  how many neighbours to attach to each row of `x`

- max_distance:

  the furthest a neighbour may be, or `NULL` for no limit

- distance:

  the name of the distance column, or `NULL` to leave it out

- suffix:

  the pair of suffixes to add to the column names both frames share

- left:

  whether to keep rows of `x` that match nothing

## Value

an Arrow table with the columns of `x`, the non geometry columns of `y`,
and the distance between each pair

## Details

`ga_knn_join()` repeats each row of `x` once per neighbour and orders
the neighbours nearest first, so the result has `nrow(x) * k` rows
unless `max_distance` rules some out. With `left = TRUE` it keeps a row
with no neighbour once, with `NA`; otherwise it drops that row.

`ga_knn_join()` measures Euclidean distance between the geometries
themselves, so it joins a point to the polygon whose edge is nearest
rather than to the one whose bounding box is. Both frames need exactly
one GeoArrow geometry column.

## See also

Other index:
[`KDTree`](https://josiahparry.github.io/geoarrowrs/reference/KDTree.md),
[`RTree`](https://josiahparry.github.io/geoarrowrs/reference/RTree.md),
[`ga_envelope()`](https://josiahparry.github.io/geoarrowrs/reference/ga_envelope.md),
[`ga_hilbert_order()`](https://josiahparry.github.io/geoarrowrs/reference/ga_hilbert_order.md),
[`ga_set_thread_pool()`](https://josiahparry.github.io/geoarrowrs/reference/ga_set_thread_pool.md),
[`ga_sparse_dwithin()`](https://josiahparry.github.io/geoarrowrs/reference/ga_sparse_dwithin.md),
[`ga_sparse_knn()`](https://josiahparry.github.io/geoarrowrs/reference/ga_sparse_knn.md),
[`ga_str_order()`](https://josiahparry.github.io/geoarrowrs/reference/ga_str_order.md)

## Examples

``` r
nc <- as.data.frame(read_shapefile(
  system.file("shape/nc.shp", package = "sf")
))
counties <- nc[c("NAME", "geometry")]
sites <- data.frame(
  site = c("a", "b"),
  geometry = geoarrow::as_geoarrow_vctr(
    ga_point(c(-78.6, -80.8), c(35.8, 35.2))
  )
)

# the two counties nearest each site
ga_knn_join(sites, counties, k = 2)[c("site", "NAME", "distance")]
#> Table
#> 4 rows x 3 columns
#> $site <string>
#> $NAME <string>
#> $distance <double>
```
