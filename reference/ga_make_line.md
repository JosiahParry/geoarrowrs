# Build a line between pairs of points

Returns a two point linestring joining each `start` to the matching
`end`. `ga_make_line()` recycles `end`, so you can pair one destination
with every origin.

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

`ga_make_line()` returns a null element for a null or empty point on
either side, so the result is always the same length as `start`. The
length of the line is
[`ga_length_euclidean()`](https://josiahparry.github.io/geoarrowrs/reference/length.md),
which equals
[`ga_dist_euclidean_pairwise()`](https://josiahparry.github.io/geoarrowrs/reference/dist_pairwise.md)
on the same pair.

## See also

Other construct:
[`ga_linestring()`](https://josiahparry.github.io/geoarrowrs/reference/ga_linestring.md),
[`ga_make_envelope()`](https://josiahparry.github.io/geoarrowrs/reference/ga_make_envelope.md),
[`ga_make_grid()`](https://josiahparry.github.io/geoarrowrs/reference/ga_make_grid.md),
[`ga_make_line_agg()`](https://josiahparry.github.io/geoarrowrs/reference/ga_make_line_agg.md),
[`ga_make_polygon()`](https://josiahparry.github.io/geoarrowrs/reference/ga_make_polygon.md),
[`ga_multilinestring()`](https://josiahparry.github.io/geoarrowrs/reference/ga_multilinestring.md),
[`ga_multipoint()`](https://josiahparry.github.io/geoarrowrs/reference/ga_multipoint.md),
[`ga_multipolygon()`](https://josiahparry.github.io/geoarrowrs/reference/ga_multipolygon.md),
[`ga_point()`](https://josiahparry.github.io/geoarrowrs/reference/ga_point.md),
[`ga_polygon()`](https://josiahparry.github.io/geoarrowrs/reference/ga_polygon.md)

## Examples

``` r
start <- ga_point(c(0, 0), c(0, 3))
end <- ga_point(c(4, 4), c(0, 3))

as.vector(ga_length_euclidean(ga_make_line(start, end)))
#> [1] 4 4
```
