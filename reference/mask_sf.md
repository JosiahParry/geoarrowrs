# Put the sf lookalikes on the search path

Attaches the geoarrowrs versions of the sf functions sf does not make
generic, such as `st_distance()` and `st_within()`, under sf's own names
and argument names. Remove them again with `detach("geoarrowrs:sf")`.

## Usage

``` r
mask_sf()
```

## Value

The attached environment, invisibly.

## Details

You do not need it alongside sdf, which makes these functions generic
and so reaches geoarrowrs by dispatch.

They take and return the GeoArrow representation rather than `sfc`, so
they never convert anything into an sf object.

`mask_sf()` does not attach the generics, because masking one would
shadow sf's own dispatch. Those generics have S3 methods on
`nanoarrow_vctr`, which `geoarrow_vctr` extends, so `st_area()` and the
rest already reach geoarrowrs through sf's generic whether or not you
call `mask_sf()`.

## Examples

``` r
nc <- as.data.frame(read_shapefile(
  system.file("shape/nc.shp", package = "sf")
))
geom <- geoarrow::as_geoarrow_vctr(nc$geometry)

mask_sf()
as.vector(st_length(geom))[1:3]
#> Error in ga_length_euclidean(as_ga(x)): Invalid argument error: Extension type name mismatch: expected geoarrow.linestring, got geoarrow.multipolygon
as.vector(st_within(ga_centroid(geom), geom))[1:3]
#> <list_of<double>[3]>
#> [[1]]
#> [1] 1
#> 
#> [[2]]
#> [1] 2
#> 
#> [[3]]
#> [1] 3
#> 
detach("geoarrowrs:sf")
```
