# Build polygons from rings

Returns one polygon per row of `shell`, with `shell` as its exterior
ring and the lines on the same row of `holes` as its holes. This matches
PostGIS `ST_MakePolygon()`.

## Usage

``` r
ga_make_polygon(shell, holes = NULL, close = TRUE)
```

## Arguments

- shell:

  a GeoArrow linestring array of exterior rings

- holes:

  a GeoArrow multilinestring array the same length as `shell`, holding
  the holes of each polygon, or `NULL` for none

- close:

  whether to close each open ring by repeating its first coordinate

## Value

a GeoArrow polygon array the same length as `shell`

## Details

With `close = TRUE`, `ga_make_polygon()` repeats the first coordinate at
the end of any ring whose last coordinate differs from it, as
[`ga_polygon()`](https://josiahparry.github.io/geoarrowrs/reference/ga_polygon.md)
does. It returns a null polygon for a null shell and for a row with a
ring of fewer than four coordinates once closed, so the result always
has the same length as `shell`. It returns an empty polygon for an empty
shell, and skips null and empty holes.

`ga_make_polygon()` keeps the z and m values and the CRS of `shell`. Use
[`ga_make_line_agg()`](https://josiahparry.github.io/geoarrowrs/reference/ga_make_line_agg.md)
to join points into the rings first.

## See also

Other construct:
[`ga_linestring()`](https://josiahparry.github.io/geoarrowrs/reference/ga_linestring.md),
[`ga_make_envelope()`](https://josiahparry.github.io/geoarrowrs/reference/ga_make_envelope.md),
[`ga_make_grid()`](https://josiahparry.github.io/geoarrowrs/reference/ga_make_grid.md),
[`ga_make_line()`](https://josiahparry.github.io/geoarrowrs/reference/ga_make_line.md),
[`ga_make_line_agg()`](https://josiahparry.github.io/geoarrowrs/reference/ga_make_line_agg.md),
[`ga_multilinestring()`](https://josiahparry.github.io/geoarrowrs/reference/ga_multilinestring.md),
[`ga_multipoint()`](https://josiahparry.github.io/geoarrowrs/reference/ga_multipoint.md),
[`ga_multipolygon()`](https://josiahparry.github.io/geoarrowrs/reference/ga_multipolygon.md),
[`ga_point()`](https://josiahparry.github.io/geoarrowrs/reference/ga_point.md),
[`ga_polygon()`](https://josiahparry.github.io/geoarrowrs/reference/ga_polygon.md)

## Examples

``` r
# join points into rings, then rings into polygons
pts <- ga_point(c(0, 10, 10, 0, 20, 30, 20), c(0, 0, 10, 10, 0, 0, 10))
rings <- ga_make_line_agg(pts, by = c(1, 1, 1, 1, 2, 2, 2))
sf::st_as_sfc(geoarrow::as_geoarrow_vctr(ga_make_polygon(rings)))
#> Geometry set for 2 features 
#> Geometry type: POLYGON
#> Dimension:     XY
#> Bounding box:  xmin: 0 ymin: 0 xmax: 30 ymax: 10
#> CRS:           NA
#> POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))
#> POLYGON ((20 0, 30 0, 20 10, 20 0))

# a square with a square hole
shell <- ga_make_line_agg(ga_point(c(0, 10, 10, 0), c(0, 0, 10, 10)))
hole <- ga_make_line_agg(ga_point(c(2, 4, 4, 2), c(2, 2, 4, 4)))
sf::st_as_sfc(geoarrow::as_geoarrow_vctr(
  ga_make_polygon(shell, ga_as_multilinestring(hole))
))
#> Geometry set for 1 feature 
#> Geometry type: POLYGON
#> Dimension:     XY
#> Bounding box:  xmin: 0 ymin: 0 xmax: 10 ymax: 10
#> CRS:           NA
#> POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0), (2 2, 4...
```
