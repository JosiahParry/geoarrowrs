# Reverse the coordinate order of geometries

Reverses the order of the coordinates in every line and ring, keeping
the geometry type of the input. `ga_reverse()` leaves the order of the
parts of a multi part geometry unchanged.

## Usage

``` r
ga_reverse(geometry)
```

## Arguments

- geometry:

  a GeoArrow geometry array

## Value

a GeoArrow array of the same geometry type as `geometry`

## Details

`ga_reverse()` returns points and multipoints, which have no order to
reverse, as they are. It flips a polygon's winding; use
[`ga_orient()`](https://josiahparry.github.io/geoarrowrs/reference/ga_orient.md)
to set a winding direction instead. It keeps a null geometry null.

## See also

Other winding:
[`ga_is_ccw()`](https://josiahparry.github.io/geoarrowrs/reference/ga_is_ccw.md),
[`ga_orient()`](https://josiahparry.github.io/geoarrowrs/reference/ga_orient.md),
[`ga_winding_order()`](https://josiahparry.github.io/geoarrowrs/reference/ga_winding_order.md)

## Examples

``` r
g <- geoarrow::as_geoarrow_array(sf::st_sfc(
  sf::st_linestring(rbind(c(0, 0), c(1, 1), c(2, 0)))
))

sf::st_as_sfc(geoarrow::as_geoarrow_vctr(ga_reverse(g)))
#> Geometry set for 1 feature 
#> Geometry type: LINESTRING
#> Dimension:     XY
#> Bounding box:  xmin: 0 ymin: 0 xmax: 2 ymax: 1
#> CRS:           NA
#> LINESTRING (2 0, 1 1, 0 0)
```
