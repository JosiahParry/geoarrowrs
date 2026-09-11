# Read the x or y of a point

Returns one coordinate per row. Any geometry that is not a single point,
and any null or empty point, gives `NA`.

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

Both coordinate encodings work, so a point array stored as a struct of
two double columns and one stored as interleaved values read the same.

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
pts <- ga_xy(c(-111.76, -112.07), c(34.87, 33.45))

as.vector(ga_x(pts))
#> [1] -111.76 -112.07
as.vector(ga_y(pts))
#> [1] 34.87 33.45
```
