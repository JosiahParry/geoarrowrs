# Build a line between pairs of points

Returns a two point linestring joining each `start` to the matching
`end`. `end` is recycled, so one destination pairs with every origin.

## Usage

``` r
ga_make_line(start, end)
```

## Arguments

- start:

  a GeoArrow point array

- end:

  a GeoArrow point array; length 1 or the same length as `start`

## Value

a GeoArrow linestring array of the same length as `start`

## Details

A null or empty point on either side gives a null element, so the result
is always the same length as `start`. The length of the line is
[`ga_length_euclidean()`](https://josiahparry.github.io/geoarrowrs/reference/length.md),
which equals
[`ga_dist_euclidean_pairwise()`](https://josiahparry.github.io/geoarrowrs/reference/dist_pairwise.md)
on the same pair.

## See also

Other construct:
[`ga_xy()`](https://josiahparry.github.io/geoarrowrs/reference/ga_xy.md)

## Examples

``` r
start <- ga_xy(c(0, 0), c(0, 3))
end <- ga_xy(c(4, 4), c(0, 3))

as.vector(ga_length_euclidean(ga_make_line(start, end)))
#> [1] 4 4
```
