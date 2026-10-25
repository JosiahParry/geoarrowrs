# Find which coordinates simplification would keep

Returns the positions of the coordinates that simplification would keep,
rather than the simplified geometry itself. One list per input geometry.

## Usage

``` r
ga_simplify_idx(geometry, epsilon)

ga_simplify_vw_idx(geometry, epsilon)
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
timestamp or a sensor reading per vertex. If you simplify the geometry
you lose that alignment; with the indices you can subset the other
columns the same way.

`ga_simplify_idx()` uses Ramer-Douglas-Peucker and
`ga_simplify_vw_idx()` uses Visvalingam-Whyatt, matching
[`ga_simplify()`](https://josiahparry.github.io/geoarrowrs/reference/ga_simplify.md)
and
[`ga_simplify_vw()`](https://josiahparry.github.io/geoarrowrs/reference/ga_simplify_vw.md).
Each function returns 1 based indices and always includes the first and
last coordinate.

Each function works on linestrings only and returns null for any other
geometry type or a null geometry.

## References

[SimplifyIdx](https://docs.rs/geo/latest/geo/algorithm/simplify/trait.SimplifyIdx.html)

[SimplifyVwIdx](https://docs.rs/geo/latest/geo/algorithm/simplify_vw/trait.SimplifyVwIdx.html)

## See also

Other simplify:
[`ga_simplify()`](https://josiahparry.github.io/geoarrowrs/reference/ga_simplify.md),
[`ga_simplify_vw()`](https://josiahparry.github.io/geoarrowrs/reference/ga_simplify_vw.md)

## Examples

``` r
line <- sf::st_linestring(cbind(c(0, 1, 2, 3, 4), c(0, 0.1, 0, 0.1, 0)))
g <- geoarrow::as_geoarrow_array(sf::st_sfc(line))

as.vector(ga_simplify_idx(g, 0.5))
#> <list_of<integer>[1]>
#> [[1]]
#> [1] 1 5
#> 
```
