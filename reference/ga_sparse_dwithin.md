# Find which rows of `y` lie within a distance of each row of `x`

Returns the row numbers of `y` whose geometry is no further than
`distance` from each row of `x`. This is the sparse form of a distance
band join, and the same test as PostGIS `ST_DWithin()`.

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

  one of `"euclidean"`, `"haversine"`, `"geodesic"`, or `"rhumb"`.
  Everything but `"euclidean"` measures between points only.

## Value

a list array of 1 based row numbers into `y`, the same length as `x`

## Details

Distance is measured between the geometries themselves, so a point
counts when it is within `distance` of the nearest edge of a polygon,
not of the box around it. This is why it differs from buffering `x` and
intersecting: a buffer approximates its curves with segments, so a point
just inside the true radius can fall outside the buffer.

`metric` decides what `distance` means. `"euclidean"` measures in the
units the coordinates are in, which on longitude and latitude is
degrees, and is what Sedona, PostGIS and DuckDB's `ST_DWithin()` all
measure. It is the default because it is the only metric `geo` defines
between geometries of any type. For longitude and latitude the answer
you almost certainly want is `"geodesic"` or `"haversine"`, which
measure metres, and which `geo` defines between points alone: a non
point geometry with either is an error rather than a planar number
wearing a spherical name.

The bounding box of each row of `x` is grown far enough to reach
`distance` before the tree is searched, so nothing within reach is
missed and only the rows that could qualify are measured. `distance` is
recycled, so one value covers every row or a different radius can apply
to each.

A row that matches nothing gives a zero length element, not a null. A
null or empty geometry in `x`, or a null `distance`, gives a null
element, and a null geometry in `y` is never returned.

## See also

Other index:
[`KDTree`](https://josiahparry.github.io/geoarrowrs/reference/KDTree.md),
[`RTree`](https://josiahparry.github.io/geoarrowrs/reference/RTree.md),
[`ga_envelope()`](https://josiahparry.github.io/geoarrowrs/reference/ga_envelope.md),
[`ga_knn_join()`](https://josiahparry.github.io/geoarrowrs/reference/ga_knn_join.md),
[`ga_set_thread_pool()`](https://josiahparry.github.io/geoarrowrs/reference/ga_set_thread_pool.md),
[`ga_sparse_knn()`](https://josiahparry.github.io/geoarrowrs/reference/ga_sparse_knn.md)

## Examples

``` r
nc <- as.data.frame(read_shapefile(
  system.file("shape/nc.shp", package = "sf")
))
sites <- ga_xy(c(-78.6, -80.8), c(35.8, 35.2))

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
other <- ga_xy(c(-78.7, -79.9), c(35.9, 35.4))
as.vector(ga_sparse_dwithin(sites, other, 50000, metric = "geodesic"))
#> <list_of<double>[2]>
#> [[1]]
#> [1] 1
#> 
#> [[2]]
#> numeric(0)
#> 
```
