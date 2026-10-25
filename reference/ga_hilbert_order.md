# Order rows along a Hilbert curve

Returns the row order that puts geometries whose bounding box centres
lie close together next to each other. Pass it to `[` to sort an array
or a data frame so that nearby rows sit together.

## Usage

``` r
ga_hilbert_order(geometry)
```

## Arguments

- geometry:

  a GeoArrow geometry array

## Value

an integer array of 1 based row numbers, the same length as `geometry`

## Details

`ga_hilbert_order()` maps each centre onto a 65536 by 65536 grid over
the extent of every row and orders the rows by their position along a
Hilbert curve through that grid, the same curve
[RTree](https://josiahparry.github.io/geoarrowrs/reference/RTree.md)
packs its tree with. If you sort a table this way before you write
GeoParquet, each row group covers a small area, so a reader can skip the
row groups that cannot match a query.

`ga_hilbert_order()` breaks ties by the original row order, and puts
rows with a null or empty geometry last, in their original order.

## See also

Other index:
[`KDTree`](https://josiahparry.github.io/geoarrowrs/reference/KDTree.md),
[`RTree`](https://josiahparry.github.io/geoarrowrs/reference/RTree.md),
[`ga_envelope()`](https://josiahparry.github.io/geoarrowrs/reference/ga_envelope.md),
[`ga_knn_join()`](https://josiahparry.github.io/geoarrowrs/reference/ga_knn_join.md),
[`ga_set_thread_pool()`](https://josiahparry.github.io/geoarrowrs/reference/ga_set_thread_pool.md),
[`ga_sparse_dwithin()`](https://josiahparry.github.io/geoarrowrs/reference/ga_sparse_dwithin.md),
[`ga_sparse_knn()`](https://josiahparry.github.io/geoarrowrs/reference/ga_sparse_knn.md),
[`ga_str_order()`](https://josiahparry.github.io/geoarrowrs/reference/ga_str_order.md)

## Examples

``` r
nc <- as.data.frame(read_shapefile(
  system.file("shape/nc.shp", package = "sf")
))
ord <- as.vector(ga_hilbert_order(nc$geometry))
head(ord)
#> [1] 84 85 68 76 77 75

# neighbouring counties now sit next to each other
head(nc[ord, "NAME"])
#> [1] "Union"        "Anson"        "Mecklenburg"  "Gaston"       "Polk"        
#> [6] "Transylvania"
```
