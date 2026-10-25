# Build multipoints from coordinate columns

Returns one multipoint per run of `multipoint_id`, holding its rows in
order.

## Usage

``` r
ga_multipoint(x, y, z = NULL, m = NULL, multipoint_id = NULL, crs = NULL)
```

## Arguments

- x, y:

  numeric vectors or float64 arrays of coordinates, one row per vertex

- z, m:

  numeric vectors or float64 arrays of z and m values, or `NULL` to
  leave out that dimension

- multipoint_id:

  a numeric vector, character vector, or Arrow array naming the
  multipoint of each row, or `NULL` for a single multipoint

- crs:

  a coordinate reference system to record, or `NULL` for none

## Value

a GeoArrow multipoint array with one element per multipoint

## Details

`ga_multipoint()` matches `sfheaders::sf_multipoint()`, taking the
columns themselves rather than a data frame and their names. You must
keep the rows of each multipoint together: `ga_multipoint()` errors when
a `multipoint_id` returns after a different one, and orders the
multipoints by where each id first appears.

`ga_multipoint()` adds the z and m dimensions when you supply `z` and
`m`. It errors on `NA` in any coordinate or id, and reuses the
coordinate arrays without copying them.

## See also

Other construct:
[`ga_linestring()`](https://josiahparry.github.io/geoarrowrs/reference/ga_linestring.md),
[`ga_make_envelope()`](https://josiahparry.github.io/geoarrowrs/reference/ga_make_envelope.md),
[`ga_make_grid()`](https://josiahparry.github.io/geoarrowrs/reference/ga_make_grid.md),
[`ga_make_line()`](https://josiahparry.github.io/geoarrowrs/reference/ga_make_line.md),
[`ga_make_line_agg()`](https://josiahparry.github.io/geoarrowrs/reference/ga_make_line_agg.md),
[`ga_make_polygon()`](https://josiahparry.github.io/geoarrowrs/reference/ga_make_polygon.md),
[`ga_multilinestring()`](https://josiahparry.github.io/geoarrowrs/reference/ga_multilinestring.md),
[`ga_multipolygon()`](https://josiahparry.github.io/geoarrowrs/reference/ga_multipolygon.md),
[`ga_point()`](https://josiahparry.github.io/geoarrowrs/reference/ga_point.md),
[`ga_polygon()`](https://josiahparry.github.io/geoarrowrs/reference/ga_polygon.md)

## Examples

``` r
pts <- ga_multipoint(c(0, 1, 5), c(0, 1, 5), multipoint_id = c("a", "a", "b"))
sf::st_as_sfc(geoarrow::as_geoarrow_vctr(pts))
#> Geometry set for 2 features 
#> Geometry type: MULTIPOINT
#> Dimension:     XY
#> Bounding box:  xmin: 0 ymin: 0 xmax: 5 ymax: 5
#> CRS:           NA
#> MULTIPOINT ((0 0), (1 1))
#> MULTIPOINT ((5 5))
```
