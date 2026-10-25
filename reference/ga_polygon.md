# Build polygons from coordinate columns

Returns one polygon per run of `polygon_id`, with one ring per run of
`linestring_id` inside it. `ga_polygon()` takes the first ring of each
polygon as its exterior and the rest as holes.

## Usage

``` r
ga_polygon(
  x,
  y,
  z = NULL,
  m = NULL,
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

- polygon_id:

  a numeric vector, character vector, or Arrow array naming the polygon
  of each row, or `NULL` for a single polygon

- linestring_id:

  a numeric vector, character vector, or Arrow array naming the ring of
  each row within its polygon, or `NULL` for one ring per polygon

- close:

  whether to close each open ring by repeating its first coordinate

- crs:

  a coordinate reference system to record, or `NULL` for none

## Value

a GeoArrow polygon array with one element per polygon

## Details

`ga_polygon()` matches `sfheaders::sf_polygon()`, taking the columns
themselves rather than a data frame and their names. It reads the rows
in order, so you must keep the rows of each polygon together.
`ga_polygon()` errors when a `polygon_id` returns after a different one
rather than merging the two runs, and it orders the polygons by where
each `polygon_id` first appears. It starts a new ring at the start of
each polygon and wherever `linestring_id` changes, so you can number the
rings of every polygon from 1.

With `close = TRUE`, `ga_polygon()` repeats the first coordinate at the
end of any ring whose last coordinate differs from it in any dimension,
and errors on a ring with fewer than four coordinates once closed. When
no ring needs closing, it reuses the coordinate arrays without copying
them.

`ga_polygon()` adds the z and m dimensions when you supply `z` and `m`.
It errors on `NA` in any coordinate or id. It stores `crs` verbatim in
the array's GeoArrow metadata and never interprets it. If you leave it
`NULL`, it produces an array with no CRS.

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
[`ga_multipolygon()`](https://josiahparry.github.io/geoarrowrs/reference/ga_multipolygon.md),
[`ga_point()`](https://josiahparry.github.io/geoarrowrs/reference/ga_point.md)

## Examples

``` r
# a square with a square hole, then a triangle
x <- c(0, 10, 10, 0, 2, 4, 4, 2, 20, 30, 20)
y <- c(0, 0, 10, 10, 2, 2, 4, 4, 0, 0, 10)
polygon_id <- c(1, 1, 1, 1, 1, 1, 1, 1, 2, 2, 2)
linestring_id <- c(1, 1, 1, 1, 2, 2, 2, 2, 1, 1, 1)

polys <- ga_polygon(x, y, polygon_id = polygon_id, linestring_id = linestring_id)
sf::st_as_sfc(geoarrow::as_geoarrow_vctr(polys))
#> Geometry set for 2 features 
#> Geometry type: POLYGON
#> Dimension:     XY
#> Bounding box:  xmin: 0 ymin: 0 xmax: 30 ymax: 10
#> CRS:           NA
#> POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0), (2 2, 4...
#> POLYGON ((20 0, 30 0, 20 10, 20 0))

# supplying z gives a POLYGON Z
sf::st_as_sfc(geoarrow::as_geoarrow_vctr(
  ga_polygon(c(0, 1, 1), c(0, 0, 1), z = c(5, 5, 6))
))
#> Geometry set for 1 feature 
#> Geometry type: POLYGON
#> Dimension:     XYZ
#> Bounding box:  xmin: 0 ymin: 0 xmax: 1 ymax: 1
#> z_range:       zmin: 5 zmax: 6
#> CRS:           NA
#> POLYGON Z ((0 0 5, 1 0 5, 1 1 6, 0 0 5))
```
