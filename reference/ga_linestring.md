# Build linestrings from coordinate columns

Returns one linestring per run of `linestring_id`, through its rows in
order.

## Usage

``` r
ga_linestring(x, y, z = NULL, m = NULL, linestring_id = NULL, crs = NULL)
```

## Arguments

- x, y:

  numeric vectors or float64 arrays of coordinates, one row per vertex

- z, m:

  numeric vectors or float64 arrays of z and m values, or `NULL` to
  leave out that dimension

- linestring_id:

  a numeric vector, character vector, or Arrow array naming the
  linestring of each row, or `NULL` for a single linestring

- crs:

  a coordinate reference system to record, or `NULL` for none

## Value

a GeoArrow linestring array with one element per linestring

## Details

`ga_linestring()` matches `sfheaders::sf_linestring()`, taking the
columns themselves rather than a data frame and their names. It reads
the rows in order, so you must keep the rows of each linestring
together: `ga_linestring()` errors when a `linestring_id` returns after
a different one, and orders the linestrings by where each id first
appears.

`ga_linestring()` adds the z and m dimensions when you supply `z` and
`m`. It errors on `NA` in any coordinate or id, and reuses the
coordinate arrays without copying them.

## See also

Other construct:
[`ga_make_envelope()`](https://josiahparry.github.io/geoarrowrs/reference/ga_make_envelope.md),
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
lines <- ga_linestring(
  c(0, 1, 2, 10, 11),
  c(0, 1, 0, 5, 5),
  linestring_id = c(1, 1, 1, 2, 2)
)
sf::st_as_sfc(geoarrow::as_geoarrow_vctr(lines))
#> Geometry set for 2 features 
#> Geometry type: LINESTRING
#> Dimension:     XY
#> Bounding box:  xmin: 0 ymin: 0 xmax: 11 ymax: 5
#> CRS:           NA
#> LINESTRING (0 0, 1 1, 2 0)
#> LINESTRING (10 5, 11 5)
```
