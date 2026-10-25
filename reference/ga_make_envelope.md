# Build rectangles from their bounds

Returns the axis aligned rectangle spanning `xmin` to `xmax` and `ymin`
to `ymax` on each row, as a polygon array. `ga_make_envelope()` recycles
any bound of length 1.

## Usage

``` r
ga_make_envelope(xmin, ymin, xmax, ymax, crs = NULL)
```

## Arguments

- xmin, ymin, xmax, ymax:

  numeric vectors or float64 arrays of the bounds; each length 1 or the
  same length as the longest

- crs:

  a coordinate reference system to record, or `NULL` for none

## Value

a GeoArrow polygon array

## Details

This matches PostGIS `ST_MakeEnvelope()`. `ga_make_envelope()` sorts
each pair of bounds, so a row with `xmin` above `xmax` gives the same
rectangle. It returns a null polygon for a row where any bound is `NA`,
so the result always has the same length as the longest bound.

`ga_make_envelope()` stores `crs` verbatim in the array's GeoArrow
metadata and never interprets it. If you leave it `NULL`, it produces an
array with no CRS.

## See also

Other construct:
[`ga_linestring()`](https://josiahparry.github.io/geoarrowrs/reference/ga_linestring.md),
[`ga_make_grid()`](https://josiahparry.github.io/geoarrowrs/reference/ga_make_grid.md),
[`ga_make_line()`](https://josiahparry.github.io/geoarrowrs/reference/ga_make_line.md),
[`ga_make_line_agg()`](https://josiahparry.github.io/geoarrowrs/reference/ga_make_line_agg.md),
[`ga_make_polygon()`](https://josiahparry.github.io/geoarrowrs/reference/ga_make_polygon.md),
[`ga_multilinestring()`](https://josiahparry.github.io/geoarrowrs/reference/ga_multilinestring.md),
[`ga_multipoint()`](https://josiahparry.github.io/geoarrowrs/reference/ga_multipoint.md),
[`ga_multipolygon()`](https://josiahparry.github.io/geoarrowrs/reference/ga_multipolygon.md),
[`ga_point()`](https://josiahparry.github.io/geoarrowrs/reference/ga_point.md),
[`ga_polygon()`](https://josiahparry.github.io/geoarrowrs/reference/ga_polygon.md)

## Examples

``` r
boxes <- ga_make_envelope(c(0, 10), c(0, 10), c(1, 12), c(2, 14))
sf::st_as_sfc(geoarrow::as_geoarrow_vctr(boxes))
#> Geometry set for 2 features 
#> Geometry type: POLYGON
#> Dimension:     XY
#> Bounding box:  xmin: 0 ymin: 0 xmax: 12 ymax: 14
#> CRS:           NA
#> POLYGON ((1 0, 1 2, 0 2, 0 0, 1 0))
#> POLYGON ((12 10, 12 14, 10 14, 10 10, 12 10))

as.vector(ga_unsigned_area(boxes))
#> [1] 2 8
```
