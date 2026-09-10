# Find the point on a geometry closest to another point

Returns the position on each geometry nearest the corresponding point,
using planar distance. `ga_closest_point_haversine()` measures on a
sphere instead, treating coordinates as longitude and latitude in
degrees.

## Usage

``` r
ga_closest_point(geometry, point)

ga_closest_point_haversine(geometry, point)
```

## Arguments

- geometry:

  a GeoArrow geometry array

- point:

  a GeoArrow point array; length 1 or the same length as `geometry`

## Value

a GeoArrow point array of the same length as `geometry`

## Details

When the point lies on the geometry the intersection itself is returned.
A geometry with no single nearest position, such as a point equidistant
from both ends of a symmetric line, becomes a null element, as does a
null geometry or a null point.

## References

[ClosestPoint](https://docs.rs/geo/latest/geo/algorithm/closest_point/trait.ClosestPoint.html)

[HaversineClosestPoint](https://docs.rs/geo/latest/geo/algorithm/haversine_closest_point/trait.HaversineClosestPoint.html)

## See also

Other query:
[`ga_interior_point()`](https://josiahparry.github.io/geoarrowrs/reference/ga_interior_point.md),
[`ga_is_convex()`](https://josiahparry.github.io/geoarrowrs/reference/ga_is_convex.md),
[`ga_line_locate_point()`](https://josiahparry.github.io/geoarrowrs/reference/ga_line_locate_point.md)

## Examples

``` r
line <- geoarrow::as_geoarrow_array(sf::st_sfc(
  sf::st_linestring(cbind(c(0, 10), c(0, 0)))
))
pt <- geoarrow::as_geoarrow_array(sf::st_sfc(sf::st_point(c(4, 5))))

sf::st_as_sfc(geoarrow::as_geoarrow_vctr(ga_closest_point(line, pt)))
#> Geometry set for 1 feature 
#> Geometry type: POINT
#> Dimension:     XY
#> Bounding box:  xmin: 4 ymin: 0 xmax: 4 ymax: 0
#> CRS:           NA
#> POINT (4 0)
```
