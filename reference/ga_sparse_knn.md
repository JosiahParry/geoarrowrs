# Find the rows of `y` nearest each row of `x`

Returns the `k` rows of `y` closest to each row of `x`, nearest first,
each paired with the distance between them. This is the sparse form of a
nearest neighbour search, and the shape
[`ga_knn_join()`](https://josiahparry.github.io/geoarrowrs/reference/ga_knn_join.md)
needs.

## Usage

``` r
ga_sparse_knn(x, y, k = 1, max_distance = NULL, metric = "euclidean")
```

## Arguments

- x:

  a GeoArrow geometry array

- y:

  a GeoArrow geometry array

- k:

  how many rows of `y` to return per row of `x`

- max_distance:

  the furthest a match may be, in the units of `metric`, or `NULL` for
  no limit

- metric:

  one of `"euclidean"`, `"haversine"`, `"geodesic"`, or `"rhumb"`.
  Everything but `"euclidean"` measures between points only.

## Value

a list array of `row` and `distance` pairs, the same length as `x`,
where `row` is a 1 based row number into `y`

## Details

Distance is measured between the geometries themselves, not between
their bounding boxes, so the nearest edge of a polygon counts rather
than the corner of the box around it. `y` is indexed in a packed Hilbert
R-tree, which narrows the search to the rows that can win before any
exact distance is computed, and the answer is the same as comparing
every pair.

`metric` decides how far apart two rows are, and so which rows win.
`"euclidean"` measures in the units the coordinates are in, which on
longitude and latitude is degrees, and ranks neighbours differently from
a real distance once the rows are far apart or near a pole. It is the
default because it is the only metric `geo` defines between geometries
of any type. For longitude and latitude points, `"geodesic"` or
`"haversine"` is the answer you want.

A row matches fewer than `k` rows only when `max_distance` rules the
rest out or `y` is shorter than `k`. A null or empty geometry in `x`
gives a null element, and one in `y` is never returned.

## See also

Other index:
[`KDTree`](https://josiahparry.github.io/geoarrowrs/reference/KDTree.md),
[`RTree`](https://josiahparry.github.io/geoarrowrs/reference/RTree.md),
[`ga_envelope()`](https://josiahparry.github.io/geoarrowrs/reference/ga_envelope.md),
[`ga_knn_join()`](https://josiahparry.github.io/geoarrowrs/reference/ga_knn_join.md),
[`ga_set_thread_pool()`](https://josiahparry.github.io/geoarrowrs/reference/ga_set_thread_pool.md),
[`ga_sparse_dwithin()`](https://josiahparry.github.io/geoarrowrs/reference/ga_sparse_dwithin.md)

## Examples

``` r
nc <- as.data.frame(read_shapefile(
  system.file("shape/nc.shp", package = "sf")
))
sites <- ga_xy(c(-78.6, -80.8), c(35.8, 35.2))

# the three counties nearest each site, with their distances
as.vector(ga_sparse_knn(sites, nc$geometry, k = 3))
#> <list_of<
#>   data.frame<
#>     row     : double
#>     distance: double
#>   >
#> >[2]>
#> [[1]]
#>   row  distance
#> 1  37 0.0000000
#> 2  54 0.1575404
#> 3  30 0.1900600
#> 
#> [[2]]
#>   row  distance
#> 1  68 0.0000000
#> 2  84 0.1540929
#> 3  69 0.1547194
#> 
```
