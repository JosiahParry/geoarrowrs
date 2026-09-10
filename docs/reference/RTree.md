# A packed Hilbert R-tree over the bounding boxes of a geometry array. A spatial index over a geometry array

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
[`intersects()`](https://josiahparry.github.io/geoarrowrs/reference/topology.md)
when exactness matters. Narrowing to candidates first is the point: the
exact test then runs on a handful of rows rather than all of them.

Rows with a null or empty geometry have no bounding box and are left out
of the tree, so a query never returns them. Row numbers always refer to
positions in the original array.

## Methods

### Method `new`

Build the index. `node_size` sets how many entries share a tree node;
larger values build faster and query slower.

### Method `search`

Find the rows whose bounding box overlaps a query box

Returns the row numbers whose bounding box intersects the given box, in
increasing order.

#### Arguments

- `xmin,ymin,xmax,ymax`:

  the query box

#### details

This is a bounding box test, not an exact one. Two geometries whose
boxes overlap need not themselves intersect, so treat the result as a
set of candidates and confirm with
[`intersects()`](https://josiahparry.github.io/geoarrowrs/reference/topology.md)
when exactness matters.

#### returns

an integer array of 1 based row numbers

#### export

#### family

index

### Method `neighbors`

Find the rows nearest a point

Returns row numbers ordered by how close their bounding box is to the
point.

#### Arguments

- `x,y`:

  the query point

- `max_results`:

  the most rows to return, or `NULL` for no limit

- `max_distance`:

  the furthest to search, or `NULL` for no limit

#### details

Distance is measured to the bounding box rather than to the geometry
itself, so this too gives candidates. `max_results` caps how many come
back and `max_distance` caps how far the search goes; either can be
`NULL`.

#### returns

an integer array of 1 based row numbers

#### export

#### family

index

### Method `size`

The number of rows the index was built over

#### returns

the length of the array the index was built from

#### export

#### family

index

### Method `n_indexed`

The number of rows actually held in the tree

#### details

Lower than `size()` when the array held null or empty geometries, which
have no bounding box to index.

#### returns

the number of indexed rows

#### export

#### family

index

## References

[geo-index](https://docs.rs/geo-index/latest/geo_index/)

## Examples

``` r
nc <- as.data.frame(read_shapefile(
  system.file("shape/nc.shp", package = "sf")
))
idx <- RTree$new(nc$geometry)

idx$size()
#> [1] 100

# candidate rows whose bounding box meets the query box
hits <- as.vector(nanoarrow::convert_array(idx$search(-79, 35, -78, 36)))
length(hits)
#> [1] 15

# the three rows nearest a point
as.vector(nanoarrow::convert_array(idx$neighbors(-79, 35, max_results = 3)))
#> [1] 82 86 94
```
