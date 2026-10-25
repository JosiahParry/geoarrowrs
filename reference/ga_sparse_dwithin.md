# Find which rows of `y` lie within a distance of each row of `x`

Returns the row numbers of `y` whose geometry is no further than
`distance` from each row of `x`. `ga_sparse_dwithin()` is the sparse
form of a distance band join and runs the same test as PostGIS
`ST_DWithin()`.

## Usage

``` r
ga_sparse_dwithin(x, y, distance, metric = "euclidean")
```

## Arguments

- x:

  a GeoArrow geometry array

- y:

  a GeoArrow geometry array

- distance:

  the furthest a row of `y` may be, in the units of `metric`; length 1
  or the same length as `x`

- metric:

  one of `"euclidean"`, `"haversine"`, `"geodesic"`, or `"rhumb"`. Use
  `"euclidean"` for geometries other than points.

## Value

a list array of 1 based row numbers into `y`, the same length as `x`

## Details

`ga_sparse_dwithin()` measures distance between the geometries
themselves, so it counts a point that lies within `distance` of the
nearest edge of a polygon, not of the box around it. That is why it
differs from buffering `x` and intersecting:
[`ga_buffer()`](https://josiahparry.github.io/geoarrowrs/reference/ga_buffer.md)
approximates curves with segments, so a point just inside the true
radius can fall outside the buffer.

`metric` sets what `distance` means. With `"euclidean"`,
`ga_sparse_dwithin()` measures in the units of the coordinates, which on
longitude and latitude is degrees, as Sedona, PostGIS and DuckDB's
`ST_DWithin()` all do. It is the default because `geo` defines no other
metric between geometries of any type. For longitude and latitude you
almost certainly want `"geodesic"` or `"haversine"`, which work in
metres but which `geo` defines between points alone:
`ga_sparse_dwithin()` raises an error for a non point geometry with
either, rather than return a planar number wearing a spherical name.

Before it searches the tree, `ga_sparse_dwithin()` grows the bounding
box of each row of `x` far enough to reach `distance`, so it misses
nothing within reach and measures only the rows that could qualify. It
recycles `distance`, so you can give one value for every row or a
different radius for each.

`ga_sparse_dwithin()` returns a zero length element for a row that
matches nothing, not a null. It returns a null element for a null or
empty geometry in `x` or a null `distance`, and never returns a null
geometry from `y`.

## See also

Other index:
[`KDTree`](https://josiahparry.github.io/geoarrowrs/reference/KDTree.md),
[`RTree`](https://josiahparry.github.io/geoarrowrs/reference/RTree.md),
[`ga_envelope()`](https://josiahparry.github.io/geoarrowrs/reference/ga_envelope.md),
[`ga_hilbert_order()`](https://josiahparry.github.io/geoarrowrs/reference/ga_hilbert_order.md),
[`ga_knn_join()`](https://josiahparry.github.io/geoarrowrs/reference/ga_knn_join.md),
[`ga_set_thread_pool()`](https://josiahparry.github.io/geoarrowrs/reference/ga_set_thread_pool.md),
[`ga_sparse_knn()`](https://josiahparry.github.io/geoarrowrs/reference/ga_sparse_knn.md),
[`ga_str_order()`](https://josiahparry.github.io/geoarrowrs/reference/ga_str_order.md)

## Examples

``` r
nc <- as.data.frame(read_shapefile(
  system.file("shape/nc.shp", package = "sf")
))
sites <- ga_point(c(-78.6, -80.8), c(35.8, 35.2))

# the counties within a quarter degree of each site
as.vector(ga_sparse_dwithin(sites, nc$geometry, 0.25))
#> <list_of<double>[2]>
#> [[1]]
#> [1] 13 24 30 37 54
#> 
#> [[2]]
#> [1] 68 69 76 84
#> 

# between points, within 50km of each other on the ellipsoid
other <- ga_point(c(-78.7, -79.9), c(35.9, 35.4))
as.vector(ga_sparse_dwithin(sites, other, 50000, metric = "geodesic"))
#> <list_of<double>[2]>
#> [[1]]
#> [1] 1
#> 
#> [[2]]
#> numeric(0)
#> 
```
