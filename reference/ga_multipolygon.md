# Build multipolygons from coordinate columns

Returns one multipolygon per run of `multipolygon_id`, with one polygon
per run of `polygon_id` inside it and one ring per run of
`linestring_id` inside each polygon.

## Usage

``` r
ga_multipolygon(
  x,
  y,
  z = NULL,
  m = NULL,
  multipolygon_id = NULL,
  polygon_id = NULL,
  linestring_id = NULL,
  close = TRUE,
  crs = NULL
)
```

## Arguments

- x, y:

  numeric vectors or float64 arrays of coordinates, one row per vertex

- z, m:

  numeric vectors or float64 arrays of z and m values, or `NULL` to
  leave out that dimension

- multipolygon_id:

  a numeric vector, character vector, or Arrow array naming the
  multipolygon of each row, or `NULL` for a single multipolygon

- polygon_id:

  a numeric vector, character vector, or Arrow array naming the polygon
  of each row within its multipolygon, or `NULL` for one polygon per
  multipolygon

- linestring_id:

  a numeric vector, character vector, or Arrow array naming the ring of
  each row within its polygon, or `NULL` for one ring per polygon

- close:

  whether to close each open ring by repeating its first coordinate

- crs:

  a coordinate reference system to record, or `NULL` for none

## Value

a GeoArrow multipolygon array with one element per multipolygon

## Details

`ga_multipolygon()` matches `sfheaders::sf_multipolygon()`, taking the
columns themselves rather than a data frame and their names. You must
keep the rows of each multipolygon together: `ga_multipolygon()` errors
when a `multipolygon_id` returns after a different one, and orders the
multipolygons by where each id first appears. It starts a new polygon at
the start of each multipolygon and wherever `polygon_id` changes, and a
new ring at the start of each polygon and wherever `linestring_id`
changes. It takes the first ring of each polygon as its exterior and the
rest as holes.

`ga_multipolygon()` closes rings the way
[`ga_polygon()`](https://josiahparry.github.io/geoarrowrs/reference/ga_polygon.md)
does, and adds the z and m dimensions when you supply `z` and `m`. It
errors on `NA` in any coordinate or id.

## See also

Other construct:
[`ga_linestring()`](https://josiahparry.github.io/geoarrowrs/reference/ga_linestring.md),
[`ga_make_envelope()`](https://josiahparry.github.io/geoarrowrs/reference/ga_make_envelope.md),
[`ga_make_grid()`](https://josiahparry.github.io/geoarrowrs/reference/ga_make_grid.md),
[`ga_make_line()`](https://josiahparry.github.io/geoarrowrs/reference/ga_make_line.md),
[`ga_make_line_agg()`](https://josiahparry.github.io/geoarrowrs/reference/ga_make_line_agg.md),
[`ga_make_polygon()`](https://josiahparry.github.io/geoarrowrs/reference/ga_make_polygon.md),
[`ga_multilinestring()`](https://josiahparry.github.io/geoarrowrs/reference/ga_multilinestring.md),
[`ga_multipoint()`](https://josiahparry.github.io/geoarrowrs/reference/ga_multipoint.md),
[`ga_point()`](https://josiahparry.github.io/geoarrowrs/reference/ga_point.md),
[`ga_polygon()`](https://josiahparry.github.io/geoarrowrs/reference/ga_polygon.md)

## Examples

``` r
# two squares in one multipolygon, then a triangle on its own
polys <- ga_multipolygon(
  c(0, 1, 1, 0, 5, 6, 6, 5, 20, 30, 20),
  c(0, 0, 1, 1, 5, 5, 6, 6, 0, 0, 10),
  multipolygon_id = c(1, 1, 1, 1, 1, 1, 1, 1, 2, 2, 2),
  polygon_id = c(1, 1, 1, 1, 2, 2, 2, 2, 1, 1, 1)
)
sf::st_as_sfc(geoarrow::as_geoarrow_vctr(polys))
#> Geometry set for 2 features 
#> Geometry type: MULTIPOLYGON
#> Dimension:     XY
#> Bounding box:  xmin: 0 ymin: 0 xmax: 30 ymax: 10
#> CRS:           NA
#> MULTIPOLYGON (((0 0, 1 0, 1 1, 0 1, 0 0)), ((5 ...
#> MULTIPOLYGON (((20 0, 30 0, 20 10, 20 0)))
```
