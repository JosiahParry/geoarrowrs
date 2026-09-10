# Locate a point along a line as a fraction of its length

Returns how far along each line the closest position to the
corresponding point lies, as a fraction between 0 and 1.

## Usage

``` r
ga_line_locate_point(geometry, point)
```

## Arguments

- geometry:

  a GeoArrow linestring array

- point:

  a GeoArrow point array; length 1 or the same length as `geometry`

## Value

a double array of the same length as `geometry`

## Details

0 is the start of the line and 1 its end, so the value multiplied by the
line's length gives a distance. Only lines and linestrings can be
located along; any other geometry type, a null geometry, a null point,
or a zero length line becomes `NA`.

## References

[LineLocatePoint](https://docs.rs/geo/latest/geo/algorithm/line_locate_point/trait.LineLocatePoint.html)

## See also

Other query:
[`ga_closest_point()`](https://josiahparry.github.io/geoarrowrs/reference/ga_closest_point.md),
[`ga_interior_point()`](https://josiahparry.github.io/geoarrowrs/reference/ga_interior_point.md),
[`ga_is_convex()`](https://josiahparry.github.io/geoarrowrs/reference/ga_is_convex.md)

## Examples

``` r
line <- geoarrow::as_geoarrow_array(sf::st_sfc(
  sf::st_linestring(cbind(c(0, 10), c(0, 0)))
))
pt <- geoarrow::as_geoarrow_array(sf::st_sfc(sf::st_point(c(2.5, 0))))

as.vector(ga_line_locate_point(line, pt))
#> [1] 0.25
```
