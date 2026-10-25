# Compute the bounding box of geometries

Returns the axis aligned bounding box of each geometry as a box array,
the type the spatial index queries take.

## Usage

``` r
ga_envelope(x)
```

## Arguments

- x:

  a GeoArrow geometry array

## Value

a GeoArrow box array of the same length as `x`

## Details

`ga_envelope()` returns a box array as it is instead of recomputing it,
so you can call it before a query whether or not the input already holds
boxes. It returns `NA` for a null or empty geometry.

PostGIS calls this `ST_Envelope()`, which returns a polygon rather than
a box.

## See also

Other boundary:
[`ga_concave_hull()`](https://josiahparry.github.io/geoarrowrs/reference/ga_concave_hull.md),
[`ga_convex_hull()`](https://josiahparry.github.io/geoarrowrs/reference/ga_convex_hull.md),
[`ga_extremes()`](https://josiahparry.github.io/geoarrowrs/reference/ga_extremes.md),
[`ga_minimum_rotated_rect()`](https://josiahparry.github.io/geoarrowrs/reference/ga_minimum_rotated_rect.md)

Other index:
[`KDTree`](https://josiahparry.github.io/geoarrowrs/reference/KDTree.md),
[`RTree`](https://josiahparry.github.io/geoarrowrs/reference/RTree.md),
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
boxes <- ga_envelope(nc$geometry)
head(geoarrow::as_geoarrow_vctr(boxes), 3)
#> <geoarrow_vctr geoarrow.box{struct}[3]>
#> [1] <POLYGON ((-81.7410736 36.2343559, -81.2398911 36.2343559, -81.2398911 >
#> [2] <POLYGON ((-81.3475418 36.3653641, -80.9034424 36.3653641, -80.9034424 >
#> [3] <POLYGON ((-80.9657745 36.2338829, -80.4353104 36.2338829, -80.4353104 >

# already boxes, so ga_envelope() returns them untouched
identical(
  as.character(geoarrow::as_geoarrow_vctr(ga_envelope(boxes))),
  as.character(geoarrow::as_geoarrow_vctr(boxes))
)
#> [1] TRUE
```
