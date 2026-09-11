# A k-d tree over a point array

Finds which points sit in a box, or near another point.

## Usage

``` r
KDTree
```

## Value

a `KDTree` object

## Methods

### Method `new`

Build the index. A bigger `node_size` builds quicker but searches
slower.

### Method `range`

Which points fall inside each geometry's box

#### Arguments

- `geometry`:

  a GeoArrow array to look up

#### returns

a list array of 1 based row numbers, the same length as `geometry`

### Method `within`

Which points are within `r` of each point, measured flat in coordinate
units

#### Arguments

- `geometry`:

  a GeoArrow point array to look up

- `r`:

  the radius; length 1 or the same length as `geometry`

#### returns

a list array of 1 based row numbers, the same length as `geometry`

### Method `size`

How many rows went in

#### returns

the length of the array the index was built from

### Method `n_indexed`

How many rows made it into the tree, skipping null and empty points

#### returns

the number of indexed rows

## References

[geo-index](https://docs.rs/geo-index/latest/geo_index/kdtree/index.html)

## See also

Other index:
[`RTree`](https://josiahparry.github.io/geoarrowrs/reference/RTree.md),
[`ga_envelope()`](https://josiahparry.github.io/geoarrowrs/reference/ga_envelope.md),
[`ga_knn_join()`](https://josiahparry.github.io/geoarrowrs/reference/ga_knn_join.md),
[`ga_set_thread_pool()`](https://josiahparry.github.io/geoarrowrs/reference/ga_set_thread_pool.md),
[`ga_sparse_dwithin()`](https://josiahparry.github.io/geoarrowrs/reference/ga_sparse_dwithin.md),
[`ga_sparse_knn()`](https://josiahparry.github.io/geoarrowrs/reference/ga_sparse_knn.md)

## Examples

``` r
fp <- system.file("shape/nc.shp", package = "sf")
nc <- as.data.frame(read_shapefile(fp))
pts <- ga_centroid(nc$geometry)
idx <- KDTree$new(pts)

# every centroid within half a degree of every other
head(as.vector(idx$within(pts, 0.5)), 3)
#> <list_of<double>[3]>
#> [[1]]
#> [1]  1  2 18 19 34
#> 
#> [[2]]
#> [1]  1  2  3 18
#> 
#> [[3]]
#> [1]  2  3 10 23
#> 

# a single lookup is an array of one
as.vector(idx$within(ga_xy(-78.6, 35.8), 0.5))[[1]]
#> [1] 24 30 37 54
```
