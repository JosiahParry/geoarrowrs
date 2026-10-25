# Order rows by sort-tile-recursive packing

Returns the row order that sort-tile-recursive (STR) packing gives the
bounding box centres of `geometry`. Pass it to `[` to sort an array or a
data frame so that nearby rows sit together.

## Usage

``` r
ga_str_order(geometry, node_size = 16)
```

## Arguments

- geometry:

  a GeoArrow geometry array

- node_size:

  how many rows share a leaf

## Value

an integer array of 1 based row numbers, the same length as `geometry`

## Details

`ga_str_order()` sorts the rows by the x of their centres, cuts them
into vertical slices of `node_size` times the square root of the number
of leaves, and sorts each slice by y. That is the leaf order an STR
packed R-tree with `node_size` entries per node uses, the same packing
`RTree$new(sort = "str")` builds.

`ga_str_order()` breaks ties by the original row order, and puts rows
with a null or empty geometry last, in their original order.

## See also

Other index:
[`KDTree`](https://josiahparry.github.io/geoarrowrs/reference/KDTree.md),
[`RTree`](https://josiahparry.github.io/geoarrowrs/reference/RTree.md),
[`ga_envelope()`](https://josiahparry.github.io/geoarrowrs/reference/ga_envelope.md),
[`ga_hilbert_order()`](https://josiahparry.github.io/geoarrowrs/reference/ga_hilbert_order.md),
[`ga_knn_join()`](https://josiahparry.github.io/geoarrowrs/reference/ga_knn_join.md),
[`ga_set_thread_pool()`](https://josiahparry.github.io/geoarrowrs/reference/ga_set_thread_pool.md),
[`ga_sparse_dwithin()`](https://josiahparry.github.io/geoarrowrs/reference/ga_sparse_dwithin.md),
[`ga_sparse_knn()`](https://josiahparry.github.io/geoarrowrs/reference/ga_sparse_knn.md)

## Examples

``` r
nc <- as.data.frame(read_shapefile(
  system.file("shape/nc.shp", package = "sf")
))
ord <- as.vector(ga_str_order(nc$geometry, node_size = 4))

# the first slice runs south to north along the west of the state
head(nc[ord, "NAME"])
#> [1] "Clay"         "Cherokee"     "Macon"        "Transylvania" "Jackson"     
#> [6] "Polk"        
```
