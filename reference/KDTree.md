# A k-d tree over a point array

Finds which points sit in a box, or near another point.

## Usage

``` r
KDTree
```

## Value

a `KDTree` object

## Details

[`write()`](https://rdrr.io/r/base/write.html) saves the index to a file
and `KDTree$read()` opens it again, so you build it once and reuse it
across sessions. With `mmap = TRUE`, `read()` maps the file instead of
reading it, so the index opens at once and each query loads only the
parts of the tree it visits.

## Methods

### Method `new`

Build the index. With a bigger `node_size` you build quicker but search
slower.

### Method `read`

Read an index that [`write()`](https://rdrr.io/r/base/write.html) saved

#### Arguments

- `path`:

  the file [`write()`](https://rdrr.io/r/base/write.html) saved

- `mmap`:

  whether to map the file rather than read it into memory

#### details

With `mmap = TRUE`, `read()` maps the file instead of reading it into
memory, so it opens at once whatever the file's size and each query
loads only the parts of the tree it visits. Do not change or delete the
file while you use a mapped index, or R may crash.

#### returns

a `KDTree` object

### Method `write`

Save the index to a file

#### Arguments

- `path`:

  the file to write

#### details

[`write()`](https://rdrr.io/r/base/write.html) saves the tree with the
row each point came from, so `KDTree$read()` restores an index that
answers every query the same way. The tree keeps the layout of the
JavaScript kdbush library.

#### returns

`NULL`

### Method `range`

Which points fall inside each geometry's box

#### Arguments

- `geometry`:

  a GeoArrow array to look up

#### returns

a list array of 1 based row numbers, the same length as `geometry`

### Method `within`

Which points are within `r` of each point, in flat coordinate units

#### Arguments

- `geometry`:

  a GeoArrow point array to look up

- `r`:

  the radius; length 1 or the same length as `geometry`

#### returns

a list array of 1 based row numbers, the same length as `geometry`

### Method `size`

How many rows you put in

#### returns

the length of the array you built the index from

### Method `n_indexed`

How many rows the tree holds, without null and empty points

#### returns

the number of indexed rows

## References

[geo-index](https://docs.rs/geo-index/latest/geo_index/kdtree/index.html)

## See also

Other index:
[`RTree`](https://josiahparry.github.io/geoarrowrs/reference/RTree.md),
[`ga_envelope()`](https://josiahparry.github.io/geoarrowrs/reference/ga_envelope.md),
[`ga_hilbert_order()`](https://josiahparry.github.io/geoarrowrs/reference/ga_hilbert_order.md),
[`ga_knn_join()`](https://josiahparry.github.io/geoarrowrs/reference/ga_knn_join.md),
[`ga_set_thread_pool()`](https://josiahparry.github.io/geoarrowrs/reference/ga_set_thread_pool.md),
[`ga_sparse_dwithin()`](https://josiahparry.github.io/geoarrowrs/reference/ga_sparse_dwithin.md),
[`ga_sparse_knn()`](https://josiahparry.github.io/geoarrowrs/reference/ga_sparse_knn.md),
[`ga_str_order()`](https://josiahparry.github.io/geoarrowrs/reference/ga_str_order.md)

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

# for a single lookup, pass an array of one
as.vector(idx$within(ga_point(-78.6, 35.8), 0.5))[[1]]
#> [1] 24 30 37 54

# save the index and open it again without reading the whole file
path <- tempfile(fileext = ".gaix")
idx$write(path)
#> NULL
saved <- KDTree$read(path, mmap = TRUE)
as.vector(saved$within(ga_point(-78.6, 35.8), 0.5))[[1]]
#> [1] 24 30 37 54
```
