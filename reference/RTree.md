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
stores on disk. It is built once and is immutable, so it pays off when a
set of geometries is queried repeatedly.

Every query is a bounding box test, not an exact one. Two geometries
whose boxes overlap need not themselves intersect, so treat results as
candidates and confirm with
[`ga_intersects()`](https://josiahparry.github.io/geoarrowrs/reference/topology.md)
when exactness matters. Narrowing to candidates first is the point: the
exact test then runs on a handful of rows rather than all of them.

Rows with a null or empty geometry have no bounding box and are left out
of the tree, so a query never returns them. Row numbers always refer to
positions in the original array.

## Methods

### Method `new`

Build the index. `node_size` sets how many entries share a tree node;
larger values build faster and query slower. `sort` picks the packing
order, either `"hilbert"` or `"str"`.

### Method `search`

Which rows have a bounding box overlapping each geometry

Returns one list of candidate row numbers per element of `geometry`, so
the result lines up row for row with the query array. This is the shape
a spatial join needs.

#### Arguments

- `geometry`:

  a GeoArrow array to look up

#### details

Anything with a bounding box works: a point array, a polygon array, or
the box array
[`ga_envelope()`](https://josiahparry.github.io/geoarrowrs/reference/ga_envelope.md)
produces. Boxes are taken as they are and everything else is reduced to
its envelope first.

This is a bounding box test, not an exact one, so each list holds
candidates to confirm with
[`ga_intersects()`](https://josiahparry.github.io/geoarrowrs/reference/topology.md)
or another predicate. A null or empty query geometry gives a null rather
than an empty list.

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

Distance is measured from the centre of each query geometry to the
bounding box of the indexed one, so this gives candidates to confirm
with
[`ga_dist_euclidean_pairwise()`](https://josiahparry.github.io/geoarrowrs/reference/dist_pairwise.md)
when exactness matters. `k` caps how many come back per row and
`max_distance` how far the search goes.

Taking an array rather than one point at a time is what makes a nearest
neighbour join one call.

#### returns

a list array of 1 based row numbers, the same length as `geometry`

### Method `size`

The number of rows the index was built over

#### returns

the length of the array the index was built from

### Method `n_indexed`

The number of rows actually held in the tree

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
[`ga_knn_join()`](https://josiahparry.github.io/geoarrowrs/reference/ga_knn_join.md),
[`ga_set_thread_pool()`](https://josiahparry.github.io/geoarrowrs/reference/ga_set_thread_pool.md),
[`ga_sparse_dwithin()`](https://josiahparry.github.io/geoarrowrs/reference/ga_sparse_dwithin.md),
[`ga_sparse_knn()`](https://josiahparry.github.io/geoarrowrs/reference/ga_sparse_knn.md)

## Examples

``` r
nc <- as.data.frame(read_shapefile(
  system.file("shape/nc.shp", package = "sf")
))
idx <- RTree$new(nc$geometry)

# candidate rows whose box meets each county, confirmed exactly
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
```
