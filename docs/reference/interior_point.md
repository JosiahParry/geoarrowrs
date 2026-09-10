# Compute a representative point inside a geometry

Returns a point guaranteed to lie on the geometry, unlike
[`centroid()`](https://josiahparry.github.io/geoarrowrs/reference/centroid.md),
which can fall outside a concave shape.

## Usage

``` r
interior_point(geometry)
```

## Arguments

- geometry:

  a GeoArrow geometry array

## Value

a GeoArrow point array of the same length as `geometry`

## Details

For a polygon this is a point on the interior, chosen from the
horizontal line closest to the centroid. An empty or null geometry
becomes a null element.

## References

[InteriorPoint](https://docs.rs/geo/latest/geo/algorithm/interior_point/trait.InteriorPoint.html)

## See also

Other query:
[`closest_point()`](https://josiahparry.github.io/geoarrowrs/reference/closest_point.md),
[`is_convex()`](https://josiahparry.github.io/geoarrowrs/reference/is_convex.md),
[`line_locate_point()`](https://josiahparry.github.io/geoarrowrs/reference/line_locate_point.md)

## Examples

``` r
g <- geoarrow::as_geoarrow_array(sf::st_sfc(sf::st_polygon(list(
  matrix(c(0, 0, 4, 0, 4, 4, 0, 4, 0, 0), ncol = 2, byrow = TRUE)
))))

sf::st_as_sfc(geoarrow::as_geoarrow_vctr(interior_point(g)))
#> Geometry set for 1 feature 
#> Geometry type: POINT
#> Dimension:     XY
#> Bounding box:  xmin: 2 ymin: 2 xmax: 2 ymax: 2
#> CRS:           NA
#> POINT (2 2)
```
