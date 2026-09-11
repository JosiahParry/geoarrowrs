# Voronoi cells from geometry vertices

Returns one multipolygon per input geometry, whose parts are the Voronoi
cells of that geometry's vertices. The output has the same length as the
input.

## Usage

``` r
ga_voronoi_cells(geometry, clip = "padded", tolerance = 0, boundary = NULL)
```

## Arguments

- geometry:

  a GeoArrow geometry array whose vertices are the sites

- clip:

  how to bound the diagram, either `"padded"` or `"envelope"`. Ignored
  when `boundary` is supplied

- tolerance:

  sites closer together than this are snapped to the same position;
  length 1 or the same length as `geometry`

- boundary:

  an optional GeoArrow polygon array to clip to; length 1 or the same
  length as `geometry`

## Value

a GeoArrow multipolygon array of the same length as `geometry`

## Details

Every vertex of a geometry is treated as a site, so a multipoint of `k`
points yields `k` cells. A Voronoi diagram is unbounded, so the cells
are clipped: `"padded"` uses a box with 50 percent padding around the
sites, matching PostGIS `ST_VoronoiPolygons`, and `"envelope"` uses
their exact bounding box. Passing `boundary` clips to an arbitrary
polygon instead, which is the usual way to cut a diagram to a study
area.

A geometry with fewer than two distinct vertices, or one whose vertices
are all collinear, has no cells and comes back as an empty multipolygon
rather than a null. Use
[`ga_voronoi_edges()`](https://josiahparry.github.io/geoarrowrs/reference/ga_voronoi_edges.md)
for the collinear case, which returns the perpendicular bisectors. A
null geometry stays null.

## References

[Voronoi](https://docs.rs/geo/latest/geo/algorithm/voronoi/trait.Voronoi.html)

## See also

Other voronoi:
[`ga_voronoi_edges()`](https://josiahparry.github.io/geoarrowrs/reference/ga_voronoi_edges.md)

## Examples

``` r
pts <- sf::st_multipoint(cbind(c(0, 1, 1, 0), c(0, 0, 1, 1)))
g <- geoarrow::as_geoarrow_array(sf::st_sfc(pts))

cells <- ga_voronoi_cells(g)
lengths(sf::st_as_sfc(geoarrow::as_geoarrow_vctr(cells)))
#> [1] 4
```
