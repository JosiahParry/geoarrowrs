# Build a grid of squares over an extent

Returns the cells of a square grid covering the bounding box of `x`, as
polygons, as the points at their centres, or as the points at their
corners. This matches
[`sf::st_make_grid()`](https://r-spatial.github.io/sf/reference/st_make_grid.html)
for square grids.

## Usage

``` r
ga_make_grid(
  x = NULL,
  cellsize = NULL,
  offset = NULL,
  n = NULL,
  crs = NULL,
  what = "polygons"
)
```

## Arguments

- x:

  a GeoArrow geometry array whose bounding box the grid covers, or
  `NULL` when you give `cellsize`, `offset`, and `n`

- cellsize:

  the width and height of a cell; length 1 or 2

- offset:

  the x and y of the lower left corner of the grid; length 2

- n:

  the number of cells across and up; length 1 or 2

- crs:

  a coordinate reference system to record, or `NULL` for the CRS of `x`

- what:

  one of `"polygons"`, `"centers"`, or `"corners"`

## Value

a GeoArrow polygon array of the cells, or a point array of their centres
or corners

## Details

`ga_make_grid()` starts the grid at `offset`, the lower left corner of
`x` unless you give it, and steps `cellsize` across and up. Without
`cellsize`, it divides the extent of `x` into `n` cells each way, `n`
being 10 by 10 unless you give it. Without `n`, it adds cells until the
grid covers `x`. With all of `cellsize`, `offset`, and `n`, it needs no
`x`, and with none of them and no `x` it returns the 10 degree grid over
the globe that sf does.

`ga_make_grid()` orders the cells along x first, then up in y, and
starts each square at its lower left corner, as sf does. It records the
CRS of `x` unless you give `crs`.

`ga_make_grid()` builds only square grids. It has no hexagonal grid,
which `sf::st_make_grid(square = FALSE)` gives.

## See also

Other construct:
[`ga_linestring()`](https://josiahparry.github.io/geoarrowrs/reference/ga_linestring.md),
[`ga_make_envelope()`](https://josiahparry.github.io/geoarrowrs/reference/ga_make_envelope.md),
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
nc <- as.data.frame(read_shapefile(
  system.file("shape/nc.shp", package = "sf")
))

# a 4 by 2 grid of cells over the state
cells <- ga_make_grid(nc$geometry, n = c(4, 2))
sf::st_as_sfc(geoarrow::as_geoarrow_vctr(cells))
#> Geometry set for 8 features 
#> Geometry type: POLYGON
#> Dimension:     XY
#> Bounding box:  xmin: -84.32385 ymin: 33.88199 xmax: -75.45698 ymax: 36.58965
#> Geodetic CRS:  NAD27
#> First 5 geometries:
#> POLYGON ((-84.32385 33.88199, -82.10713 33.8819...
#> POLYGON ((-82.10713 33.88199, -79.89042 33.8819...
#> POLYGON ((-79.89042 33.88199, -77.6737 33.88199...
#> POLYGON ((-77.6737 33.88199, -75.45698 33.88199...
#> POLYGON ((-84.32385 35.23582, -82.10713 35.2358...

# half degree cells, and the point at the centre of each
centres <- ga_make_grid(nc$geometry, cellsize = 0.5, what = "centers")
centres$length
#> [1] 108
```
