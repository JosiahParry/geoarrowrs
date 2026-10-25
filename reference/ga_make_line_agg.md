# Join the points of each group into a line

Returns one linestring through the points that share each label of `by`,
taking the points in row order. With `by = NULL`, `ga_make_line_agg()`
joins every point into a single line.

## Usage

``` r
ga_make_line_agg(points, by = NULL)
```

## Arguments

- points:

  a GeoArrow point array

- by:

  a character vector, numeric vector, or Arrow array with one label per
  point, or `NULL` for a single line

## Value

a GeoArrow linestring array with one element per label of `by`

## Details

`ga_make_line_agg()` groups the rows the way
[`ga_collect_agg()`](https://josiahparry.github.io/geoarrowrs/reference/ga_collect_agg.md)
does. It accepts an unsorted `by`, treats `NA` as a label like any
other, and orders the lines by where each label first appears, so you
can name the lines with `unique(by)`.

`ga_make_line_agg()` skips null and empty points, and returns a null
line for a group with fewer than two points left. It keeps the z and m
values and the CRS of `points`. This is the aggregate form of PostGIS
`ST_MakeLine()`; use
[`ga_make_line()`](https://josiahparry.github.io/geoarrowrs/reference/ga_make_line.md)
to join pairs of points instead.

## See also

Other construct:
[`ga_linestring()`](https://josiahparry.github.io/geoarrowrs/reference/ga_linestring.md),
[`ga_make_envelope()`](https://josiahparry.github.io/geoarrowrs/reference/ga_make_envelope.md),
[`ga_make_grid()`](https://josiahparry.github.io/geoarrowrs/reference/ga_make_grid.md),
[`ga_make_line()`](https://josiahparry.github.io/geoarrowrs/reference/ga_make_line.md),
[`ga_make_polygon()`](https://josiahparry.github.io/geoarrowrs/reference/ga_make_polygon.md),
[`ga_multilinestring()`](https://josiahparry.github.io/geoarrowrs/reference/ga_multilinestring.md),
[`ga_multipoint()`](https://josiahparry.github.io/geoarrowrs/reference/ga_multipoint.md),
[`ga_multipolygon()`](https://josiahparry.github.io/geoarrowrs/reference/ga_multipolygon.md),
[`ga_point()`](https://josiahparry.github.io/geoarrowrs/reference/ga_point.md),
[`ga_polygon()`](https://josiahparry.github.io/geoarrowrs/reference/ga_polygon.md)

## Examples

``` r
pts <- ga_point(c(0, 1, 2, 10, 11), c(0, 1, 0, 5, 5))
lines <- ga_make_line_agg(pts, by = c("a", "a", "a", "b", "b"))
sf::st_as_sfc(geoarrow::as_geoarrow_vctr(lines))
#> Geometry set for 2 features 
#> Geometry type: LINESTRING
#> Dimension:     XY
#> Bounding box:  xmin: 0 ymin: 0 xmax: 11 ymax: 5
#> CRS:           NA
#> LINESTRING (0 0, 1 1, 2 0)
#> LINESTRING (10 5, 11 5)

as.vector(ga_length_euclidean(lines))
#> [1] 2.828427 1.000000
```
