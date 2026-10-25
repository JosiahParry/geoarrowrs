# Build a point array from coordinates

Pairs numeric vectors of coordinates into a GeoArrow point array, adding
the z and m dimensions when you supply `z` and `m`. `ga_point()`
recycles any coordinate of length 1.

## Usage

``` r
ga_point(x, y, z = NULL, m = NULL, crs = NULL)
```

## Arguments

- x, y:

  numeric vectors or float64 arrays of x and y coordinates; each length
  1 or the same length as the longest coordinate

- z, m:

  numeric vectors or float64 arrays of z and m values, or `NULL` to
  leave out that dimension

- crs:

  a coordinate reference system to record, or `NULL` for none

## Value

a GeoArrow point array

## Details

`ga_point()` returns a null point for a row where any coordinate is
`NA`, rather than a point at an arbitrary location, so the result always
has the same length as the longest input. When every coordinate is full
length and has no `NA`, it reuses the arrays without copying them.

`ga_point()` stores `crs` verbatim in the array's GeoArrow metadata and
never interprets it, so it never reprojects. It accepts anything a
reader would produce, whether an authority code such as `"EPSG:4326"`,
WKT, or PROJJSON. If you leave it `NULL`, `ga_point()` produces an array
with no CRS.

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
[`ga_polygon()`](https://josiahparry.github.io/geoarrowrs/reference/ga_polygon.md)

## Examples

``` r
pts <- ga_point(c(0, 1, 2), c(0, 1, 4), crs = "EPSG:4326")
sf::st_as_sfc(geoarrow::as_geoarrow_vctr(pts))
#> Geometry set for 3 features 
#> Geometry type: POINT
#> Dimension:     XY
#> Bounding box:  xmin: 0 ymin: 0 xmax: 2 ymax: 4
#> Geodetic CRS:  WGS 84
#> POINT (0 0)
#> POINT (1 1)
#> POINT (2 4)

# ga_point() recycles a length 1 coordinate
sf::st_as_sfc(geoarrow::as_geoarrow_vctr(ga_point(c(0, 1, 2), 0)))
#> Geometry set for 3 features 
#> Geometry type: POINT
#> Dimension:     XY
#> Bounding box:  xmin: 0 ymin: 0 xmax: 2 ymax: 0
#> CRS:           NA
#> POINT (0 0)
#> POINT (1 0)
#> POINT (2 0)

# supplying z gives a POINT Z
sf::st_as_sfc(geoarrow::as_geoarrow_vctr(ga_point(c(0, 1), c(0, 1), z = c(5, 6))))
#> Geometry set for 2 features 
#> Geometry type: POINT
#> Dimension:     XYZ
#> Bounding box:  xmin: 0 ymin: 0 xmax: 1 ymax: 1
#> z_range:       zmin: 5 zmax: 6
#> CRS:           NA
#> POINT Z (0 0 5)
#> POINT Z (1 1 6)

# ga_point() returns a null point for NA in any coordinate
ga_point(c(0, NA), c(0, 1))$null_count
#> [1] 1
```
