# A spatial index over a geometry array

Indexes the bounding box of each geometry so that queries can skip the
rows that cannot match.

## Usage

``` r
RTree
```

## Value

an `RTree` object

## Details

The tree is a packed Hilbert R-tree, the same structure FlatGeobuf
stores on disk. You build it once and it is immutable, so it pays off
when you query a set of geometries repeatedly.

Every query is a bounding box test, not an exact one. Two geometries
whose boxes overlap need not themselves intersect, so treat results as
candidates and confirm with
[`ga_intersects()`](https://josiahparry.github.io/geoarrowrs/reference/topology.md)
when exactness matters. You narrow to candidates first so you can run
the exact test on a handful of rows rather than all of them.

Rows with a null or empty geometry have no bounding box, so `new()`
leaves them out of the tree and a query never returns them. Row numbers
always refer to positions in the original array.

[`write()`](https://rdrr.io/r/base/write.html) saves the index to a file
and `RTree$read()` opens it again, so you build it once and reuse it
across sessions. With `mmap = TRUE`, `read()` maps the file instead of
reading it, so the index opens at once and each query loads only the
parts of the tree it visits.

## Methods

### Method `new`

Build the index. Use `node_size` to set how many entries share a tree
node; with larger values you build faster and query slower. Use `sort`
to pick the packing order, either `"hilbert"` or `"str"`.

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

an `RTree` object

### Method `write`

Save the index to a file

#### Arguments

- `path`:

  the file to write

#### details

[`write()`](https://rdrr.io/r/base/write.html) saves the tree with the
row each entry came from, so `RTree$read()` restores an index that
answers every query the same way. The tree keeps the layout of the
JavaScript flatbush library.

#### returns

`NULL`

### Method `search`

Which rows have a bounding box overlapping each geometry

Returns one list of candidate row numbers per element of `geometry`, row
for row with the query array. This is the shape a spatial join needs.

#### Arguments

- `geometry`:

  a GeoArrow array to look up

#### details

[`search()`](https://rdrr.io/r/base/search.html) accepts anything with a
bounding box: a point array, a polygon array, or the box array
[`ga_envelope()`](https://josiahparry.github.io/geoarrowrs/reference/ga_envelope.md)
produces. It takes boxes as they are and reduces everything else to its
envelope first.

This is a bounding box test, not an exact one, so each list holds
candidates to confirm with
[`ga_intersects()`](https://josiahparry.github.io/geoarrowrs/reference/topology.md)
or another predicate. [`search()`](https://rdrr.io/r/base/search.html)
returns a null rather than an empty list for a null or empty query
geometry.

#### returns

a list array of 1 based row numbers, the same length as `geometry`

### Method `neighbors`

Which rows are nearest each geometry, closest first

#### Arguments

- `geometry`:

  a GeoArrow array to look up

- `k`:

  the most rows to return per query, or `NULL` for no limit

- `max_distance`:

  the furthest to search, or `NULL` for no limit

#### details

`neighbors()` measures distance from the centre of each query geometry
to the bounding box of the indexed one, so it returns candidates to
confirm with
[`ga_dist_euclidean_pairwise()`](https://josiahparry.github.io/geoarrowrs/reference/dist_pairwise.md)
when exactness matters. Use `k` to cap how many rows it returns per row
and `max_distance` to cap how far it searches.

Because `neighbors()` takes an array rather than one point at a time,
you can do a nearest neighbour join in one call.

#### returns

a list array of 1 based row numbers, the same length as `geometry`

### Method `size`

The number of rows you built the index over

#### returns

the length of the array you built the index from

### Method `n_indexed`

The number of rows the tree actually holds

#### details

Lower than `size()` when the array held null or empty geometries, which
have no bounding box to index.

#### returns

the number of indexed rows

## References

[geo-index](https://docs.rs/geo-index/latest/geo_index/)

## See also

Other index:
[`KDTree`](https://josiahparry.github.io/geoarrowrs/reference/KDTree.md),
[`ga_envelope()`](https://josiahparry.github.io/geoarrowrs/reference/ga_envelope.md),
[`ga_hilbert_order()`](https://josiahparry.github.io/geoarrowrs/reference/ga_hilbert_order.md),
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
idx <- RTree$new(nc$geometry)

# candidate rows whose box meets each county, still to confirm exactly
head(as.vector(idx$search(nc$geometry)), 3)
#> <list_of<double>[3]>
#> [[1]]
#> [1]  1  2 18 19 22
#> 
#> [[2]]
#> [1]  1  2  3 18
#> 
#> [[3]]
#> [1]  2  3 10 18 23 25
#> 

# the three counties nearest each centroid
head(as.vector(idx$neighbors(ga_centroid(nc$geometry), k = 3)), 3)
#> <list_of<double>[3]>
#> [[1]]
#> [1]  1 18 19
#> 
#> [[2]]
#> [1]  2 18  1
#> 
#> [[3]]
#> [1]  3 23 18
#> 

# save the index and open it again without reading the whole file
path <- tempfile(fileext = ".gaix")
idx$write(path)
#> NULL
saved <- RTree$read(path, mmap = TRUE)
head(as.vector(saved$search(nc$geometry)), 3)
#> <list_of<double>[3]>
#> [[1]]
#> [1]  1  2 18 19 22
#> 
#> [[2]]
#> [1]  1  2  3 18
#> 
#> [[3]]
#> [1]  2  3 10 18 23 25
#> 
```
