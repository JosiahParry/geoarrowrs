# Find which coordinates simplification would keep

Returns the positions of the coordinates that survive simplification,
rather than the simplified geometry itself. One list per input geometry.

## Usage

``` r
simplify_idx(geometry, epsilon)

simplify_vw_idx(geometry, epsilon)
```

## Arguments

- geometry:

  a GeoArrow linestring array

- epsilon:

  the simplification tolerance; length 1 or the same length as
  `geometry`

## Value

a list array of integer positions, one list per input geometry

## Details

Use these when the coordinates carry data of their own, such as a
timestamp or a sensor reading per vertex. Simplifying the geometry
discards that alignment; the indices let you subset the other columns
the same way.

`simplify_idx()` uses Ramer-Douglas-Peucker and `simplify_vw_idx()` uses
Visvalingam-Whyatt, matching
[`simplify()`](https://josiahparry.github.io/geoarrowrs/reference/simplify.md)
and
[`simplify_vw()`](https://josiahparry.github.io/geoarrowrs/reference/simplify_vw.md).
Indices are 1 based and always include the first and last coordinate.

Only linestrings can be simplified this way. Any other geometry type, or
a null geometry, comes back null.

## References

[SimplifyIdx](https://docs.rs/geo/latest/geo/algorithm/simplify/trait.SimplifyIdx.html)

[SimplifyVwIdx](https://docs.rs/geo/latest/geo/algorithm/simplify_vw/trait.SimplifyVwIdx.html)

## See also

Other simplify:
[`simplify()`](https://josiahparry.github.io/geoarrowrs/reference/simplify.md),
[`simplify_vw()`](https://josiahparry.github.io/geoarrowrs/reference/simplify_vw.md)

## Examples

``` r
line <- sf::st_linestring(cbind(c(0, 1, 2, 3, 4), c(0, 0.1, 0, 0.1, 0)))
g <- geoarrow::as_geoarrow_array(sf::st_sfc(line))

nanoarrow::convert_array(simplify_idx(g, 0.5))
#> <list_of<integer>[1]>
#> [[1]]
#> [1] 1 5
#> 
```
