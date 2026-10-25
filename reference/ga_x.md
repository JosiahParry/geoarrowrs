# Read the x or y of a point

Returns one coordinate per row. Each function returns `NA` for any
geometry that is not a single point, and for any null or empty point.

## Usage

``` r
ga_x(geometry)

ga_y(geometry)
```

## Arguments

- geometry:

  a GeoArrow point array

## Value

a double array of the same length as `geometry`

## Details

Both functions accept either coordinate encoding and return the same
values whether the point array holds a struct of two double columns or
interleaved values.

Use
[`ga_coords()`](https://josiahparry.github.io/geoarrowrs/reference/ga_coords.md)
for the vertices of a line or polygon, which returns a multipoint per
row rather than a single number.

## See also

Other iteration:
[`ga_coords()`](https://josiahparry.github.io/geoarrowrs/reference/ga_coords.md),
[`ga_lines()`](https://josiahparry.github.io/geoarrowrs/reference/ga_lines.md),
[`ga_n_coords()`](https://josiahparry.github.io/geoarrowrs/reference/ga_n_coords.md)

## Examples

``` r
pts <- ga_point(c(-111.76, -112.07), c(34.87, 33.45))

as.vector(ga_x(pts))
#> [1] -111.76 -112.07
as.vector(ga_y(pts))
#> [1] 34.87 33.45
```
