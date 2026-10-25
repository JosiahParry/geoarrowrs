# Build multilinestrings from coordinate columns

Returns one multilinestring per run of `multilinestring_id`, with one
line per run of `linestring_id` inside it.

## Usage

``` r
ga_multilinestring(
  x,
  y,
  z = NULL,
  m = NULL,
  multilinestring_id = NULL,
  linestring_id = NULL,
  crs = NULL
)
```

## Arguments

- x, y:

  numeric vectors or float64 arrays of coordinates, one row per vertex

- z, m:

  numeric vectors or float64 arrays of z and m values, or `NULL` to
  leave out that dimension

- multilinestring_id:

  a numeric vector, character vector, or Arrow array naming the
  multilinestring of each row, or `NULL` for a single multilinestring

- linestring_id:

  a numeric vector, character vector, or Arrow array naming the line of
  each row within its multilinestring, or `NULL` for one line per
  multilinestring

- crs:

  a coordinate reference system to record, or `NULL` for none

## Value

a GeoArrow multilinestring array with one element per multilinestring

## Details

`ga_multilinestring()` matches `sfheaders::sf_multilinestring()`, taking
the columns themselves rather than a data frame and their names. You
must keep the rows of each multilinestring together:
`ga_multilinestring()` errors when a `multilinestring_id` returns after
a different one, and orders the multilinestrings by where each id first
appears. It starts a new line at the start of each multilinestring and
wherever `linestring_id` changes, so you can number the lines of every
multilinestring from 1.

`ga_multilinestring()` adds the z and m dimensions when you supply `z`
and `m`. It errors on `NA` in any coordinate or id, and reuses the
coordinate arrays without copying them.

## See also

Other construct:
[`ga_linestring()`](https://josiahparry.github.io/geoarrowrs/reference/ga_linestring.md),
[`ga_make_envelope()`](https://josiahparry.github.io/geoarrowrs/reference/ga_make_envelope.md),
[`ga_make_grid()`](https://josiahparry.github.io/geoarrowrs/reference/ga_make_grid.md),
[`ga_make_line()`](https://josiahparry.github.io/geoarrowrs/reference/ga_make_line.md),
[`ga_make_line_agg()`](https://josiahparry.github.io/geoarrowrs/reference/ga_make_line_agg.md),
[`ga_make_polygon()`](https://josiahparry.github.io/geoarrowrs/reference/ga_make_polygon.md),
[`ga_multipoint()`](https://josiahparry.github.io/geoarrowrs/reference/ga_multipoint.md),
[`ga_multipolygon()`](https://josiahparry.github.io/geoarrowrs/reference/ga_multipolygon.md),
[`ga_point()`](https://josiahparry.github.io/geoarrowrs/reference/ga_point.md),
[`ga_polygon()`](https://josiahparry.github.io/geoarrowrs/reference/ga_polygon.md)

## Examples

``` r
lines <- ga_multilinestring(
  c(0, 1, 5, 6, 10, 11),
  c(0, 1, 5, 6, 0, 1),
  multilinestring_id = c(1, 1, 1, 1, 2, 2),
  linestring_id = c(1, 1, 2, 2, 1, 1)
)
sf::st_as_sfc(geoarrow::as_geoarrow_vctr(lines))
#> Geometry set for 2 features 
#> Geometry type: MULTILINESTRING
#> Dimension:     XY
#> Bounding box:  xmin: 0 ymin: 0 xmax: 11 ymax: 6
#> CRS:           NA
#> MULTILINESTRING ((0 0, 1 1), (5 5, 6 6))
#> MULTILINESTRING ((10 0, 11 1))
```
