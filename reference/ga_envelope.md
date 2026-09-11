# Compute the bounding box of geometries

Returns the axis aligned bounding box of each geometry, as the box array
that the spatial index queries take.

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

A box array is passed straight through rather than recomputed, so this
is cheap to call on something that already holds boxes and safe to call
defensively before a query.

This is the envelope in the OGC sense. It differs from
[`ga_bounding_rect()`](https://josiahparry.github.io/geoarrowrs/reference/ga_bounding_rect.md)
only in that pass through; the boxes themselves are the same.

## See also

Other index:
[`KDTree`](https://josiahparry.github.io/geoarrowrs/reference/KDTree.md),
[`RTree`](https://josiahparry.github.io/geoarrowrs/reference/RTree.md),
[`ga_knn_join()`](https://josiahparry.github.io/geoarrowrs/reference/ga_knn_join.md),
[`ga_set_thread_pool()`](https://josiahparry.github.io/geoarrowrs/reference/ga_set_thread_pool.md),
[`ga_sparse_dwithin()`](https://josiahparry.github.io/geoarrowrs/reference/ga_sparse_dwithin.md),
[`ga_sparse_knn()`](https://josiahparry.github.io/geoarrowrs/reference/ga_sparse_knn.md)

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

# already boxes, so this is a no op
identical(
  as.character(geoarrow::as_geoarrow_vctr(ga_envelope(boxes))),
  as.character(geoarrow::as_geoarrow_vctr(boxes))
)
#> [1] TRUE
```
