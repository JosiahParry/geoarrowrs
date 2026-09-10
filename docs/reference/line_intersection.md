# Intersect pairs of two point lines

Returns where each pair of lines meets: a point when they cross, and a
line when they overlap along a shared stretch. `y` is recycled against
`x`.

## Usage

``` r
line_intersection(x, y)
```

## Arguments

- x:

  a GeoArrow array of two point linestrings

- y:

  a GeoArrow array of two point linestrings; length 1 or the same length
  as `x`

## Value

a GeoArrow multipoint array of the same length as `x`

## Details

The answer is a point when the lines cross and a segment when they
overlap. Both come back as a multipoint so the column has one type: a
crossing gives one point, an overlap gives the two endpoints of the
shared stretch. So
[`n_coords()`](https://josiahparry.github.io/geoarrowrs/reference/n_coords.md)
tells the two apart, and an overlap can be rebuilt from its endpoints.

Both arguments must hold single segments, that is a `LINE` or a two
point `LINESTRING`. Longer linestrings, other geometry types, and null
rows come back null, as do pairs that simply do not meet. Use
[`self_intersections()`](https://josiahparry.github.io/geoarrowrs/reference/self_intersections.md)
for a geometry with many segments.

## References

[line_intersection](https://docs.rs/geo/latest/geo/algorithm/line_intersection/fn.line_intersection.html)

## See also

Other intersection:
[`self_intersections()`](https://josiahparry.github.io/geoarrowrs/reference/self_intersections.md)

## Examples

``` r
a <- sf::st_linestring(cbind(c(0, 2), c(0, 2)))
b <- sf::st_linestring(cbind(c(0, 2), c(2, 0)))
x <- geoarrow::as_geoarrow_array(sf::st_sfc(a))
y <- geoarrow::as_geoarrow_array(sf::st_sfc(b))

sf::st_as_sfc(geoarrow::as_geoarrow_vctr(line_intersection(x, y)))
#> Geometry set for 1 feature 
#> Geometry type: MULTIPOINT
#> Dimension:     XY
#> Bounding box:  xmin: 1 ymin: 1 xmax: 1 ymax: 1
#> CRS:           NA
#> MULTIPOINT ((1 1))
```
