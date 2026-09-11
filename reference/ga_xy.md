# Build a point array from x and y coordinates

Pairs two numeric vectors into a GeoArrow point array. The shorter of
the two is recycled when it is length 1.

## Usage

``` r
ga_xy(x, y, crs = NULL)
```

## Arguments

- x:

  a numeric vector or float64 array of x coordinates

- y:

  a numeric vector or float64 array of y coordinates; length 1 or the
  same length as `x`

- crs:

  a coordinate reference system to record, or `NULL` for none

## Value

a GeoArrow point array

## Details

A row where either coordinate is `NA` gives a null point rather than a
point at an arbitrary location, so the result always has the same length
as the longer input.

`crs` is stored verbatim in the array's GeoArrow metadata and is never
interpreted, so nothing here reprojects. Anything a reader would produce
works, whether an authority code such as `"EPSG:4326"`, WKT, or
PROJJSON. Leaving it `NULL` produces an array with no CRS.

## See also

Other construct:
[`ga_make_line()`](https://josiahparry.github.io/geoarrowrs/reference/ga_make_line.md)

## Examples

``` r
pts <- ga_xy(c(0, 1, 2), c(0, 1, 4), crs = "EPSG:4326")
sf::st_as_sfc(geoarrow::as_geoarrow_vctr(pts))
#> Geometry set for 3 features 
#> Geometry type: POINT
#> Dimension:     XY
#> Bounding box:  xmin: 0 ymin: 0 xmax: 2 ymax: 4
#> Geodetic CRS:  WGS 84
#> POINT (0 0)
#> POINT (1 1)
#> POINT (2 4)

# a length 1 coordinate is recycled
sf::st_as_sfc(geoarrow::as_geoarrow_vctr(ga_xy(c(0, 1, 2), 0)))
#> Geometry set for 3 features 
#> Geometry type: POINT
#> Dimension:     XY
#> Bounding box:  xmin: 0 ymin: 0 xmax: 2 ymax: 0
#> CRS:           NA
#> POINT (0 0)
#> POINT (1 0)
#> POINT (2 0)

# NA in either coordinate gives a null point
ga_xy(c(0, NA), c(0, 1))$null_count
#> [1] 1
```
