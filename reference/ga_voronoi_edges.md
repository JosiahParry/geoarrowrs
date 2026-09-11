# Voronoi edges from geometry vertices

Returns one multilinestring per input geometry, whose parts are the
boundaries between that geometry's Voronoi cells. The output has the
same length as the input.

## Usage

``` r
ga_voronoi_edges(geometry, clip = "padded", tolerance = 0, boundary = NULL)
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

a GeoArrow multilinestring array of the same length as `geometry`

## Details

Unlike
[`ga_voronoi_cells()`](https://josiahparry.github.io/geoarrowrs/reference/ga_voronoi_cells.md),
this works on collinear sites, where the edges are the perpendicular
bisectors between neighbouring points. Prefer it when you want the
diagram's skeleton rather than closed regions.

A geometry with fewer than two distinct vertices has no edges and comes
back as an empty multilinestring. A null geometry stays null.

## References

[Voronoi](https://docs.rs/geo/latest/geo/algorithm/voronoi/trait.Voronoi.html)

## See also

Other voronoi:
[`ga_voronoi_cells()`](https://josiahparry.github.io/geoarrowrs/reference/ga_voronoi_cells.md)

## Examples

``` r
pts <- sf::st_multipoint(cbind(c(0, 1, 1, 0), c(0, 0, 1, 1)))
g <- geoarrow::as_geoarrow_array(sf::st_sfc(pts))

sf::st_as_sfc(geoarrow::as_geoarrow_vctr(ga_voronoi_edges(g)))
#> Geometry set for 1 feature 
#> Geometry type: MULTILINESTRING
#> Dimension:     XY
#> Bounding box:  xmin: -0.5 ymin: -0.5 xmax: 1.5 ymax: 1.5
#> CRS:           NA
#> MULTILINESTRING ((0.5 0.5, 0.5 -0.5), (0.5 0.5,...
```
