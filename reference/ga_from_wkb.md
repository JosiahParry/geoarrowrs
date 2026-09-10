# Read well known binary as a GeoArrow array

Parses a binary column of WKB into the narrowest GeoArrow type that fits
every geometry in it. A column of points becomes a `point` array.

## Usage

``` r
ga_from_wkb(x, crs = NULL)
```

## Arguments

- x:

  a binary array of WKB, or any GeoArrow array

- crs:

  a coordinate reference system to record, or `NULL` to keep the one `x`
  already has

## Value

a GeoArrow array of the narrowest type that fits, the same length as `x`

## Details

GeoParquet writers, and plenty of plain Parquet ones, store geometry as
a bare `binary` column with no GeoArrow extension metadata on it. Most
geoarrowrs functions read that directly, but the ones that need a
specific geometry type, such as
[`ga_dist_euclidean_pairwise()`](https://josiahparry.github.io/geoarrowrs/reference/dist_pairwise.md),
cannot tell what is in it. Converting once up front settles the type for
everything downstream.

`crs` is stored verbatim in the array's metadata and is never
interpreted, so nothing here reprojects. Leaving it `NULL` keeps
whatever the input carried, which for a bare binary column is no CRS at
all.

Mixed geometry types are left as a `geometry` array, since no narrower
type fits.

## See also

Other cast:
[`ga_as_point()`](https://josiahparry.github.io/geoarrowrs/reference/ga_as_point.md),
[`ga_cast_geometry()`](https://josiahparry.github.io/geoarrowrs/reference/ga_cast_geometry.md),
[`ga_downcast_geometry()`](https://josiahparry.github.io/geoarrowrs/reference/ga_downcast_geometry.md),
[`ga_explode()`](https://josiahparry.github.io/geoarrowrs/reference/ga_explode.md),
[`ga_flatten()`](https://josiahparry.github.io/geoarrowrs/reference/ga_flatten.md)

## Examples

``` r
sfc <- sf::st_sfc(sf::st_point(c(0, 0)), sf::st_point(c(3, 4)))
wkb <- nanoarrow::as_nanoarrow_array(
  arrow::Array$create(unclass(wk::as_wkb(sfc)), type = arrow::binary())
)

pts <- ga_from_wkb(wkb, crs = "EPSG:4326")
sf::st_as_sfc(geoarrow::as_geoarrow_vctr(pts))
#> Geometry set for 2 features 
#> Geometry type: POINT
#> Dimension:     XY
#> Bounding box:  xmin: 0 ymin: 0 xmax: 3 ymax: 4
#> Geodetic CRS:  WGS 84
#> POINT (0 0)
#> POINT (3 4)
```
