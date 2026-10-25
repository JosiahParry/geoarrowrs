# Apply a winding direction to polygon rings

Rewinds each polygon so its exterior and interior rings follow a
consistent direction. `ga_orient()` keeps the geometry type of the
input.

## Usage

``` r
ga_orient(geometry, direction = "default")
```

## Arguments

- geometry:

  a GeoArrow polygon or multipolygon array

- direction:

  one of `"default"`, `"reversed"`, `"ccw"`, or `"cw"`

## Value

a GeoArrow array of the same geometry type as `geometry`

## Details

With `"default"`, `ga_orient()` winds the exterior ring
counter-clockwise and the interior rings clockwise, which is the winding
the OGC simple features and GeoJSON specifications call for. With
`"reversed"` it does the opposite. It accepts `"ccw"` and `"cw"` as
aliases, which refer to the exterior ring.

`ga_orient()` does not change which points a polygon covers, only the
order of its coordinates. It keeps a null geometry null.

## References

[Orient](https://docs.rs/geo/latest/geo/algorithm/orient/trait.Orient.html)

## See also

Other winding:
[`ga_is_ccw()`](https://josiahparry.github.io/geoarrowrs/reference/ga_is_ccw.md),
[`ga_reverse()`](https://josiahparry.github.io/geoarrowrs/reference/ga_reverse.md),
[`ga_winding_order()`](https://josiahparry.github.io/geoarrowrs/reference/ga_winding_order.md)

## Examples

``` r
# a clockwise square
ring <- rbind(c(0, 0), c(0, 1), c(1, 1), c(1, 0), c(0, 0))
p <- sf::st_polygon(list(ring))
g <- geoarrow::as_geoarrow_array(sf::st_sfc(p))

ga_winding_order(g)
#> <nanoarrow_array string[1]>
#>  $ length    : int 1
#>  $ null_count: int 0
#>  $ offset    : int 0
#>  $ buffers   :List of 3
#>   ..$ :<nanoarrow_buffer validity<bool>[null] ``
#>   ..$ :<nanoarrow_buffer data_offset<int32>[2][8 b]> `0 9`
#>   ..$ :<nanoarrow_buffer data<string>[9 b]> `clockwise`
#>  $ dictionary: NULL
#>  $ children  : list()
ga_winding_order(ga_orient(g, "default"))
#> <nanoarrow_array string[1]>
#>  $ length    : int 1
#>  $ null_count: int 0
#>  $ offset    : int 0
#>  $ buffers   :List of 3
#>   ..$ :<nanoarrow_buffer validity<bool>[null] ``
#>   ..$ :<nanoarrow_buffer data_offset<int32>[2][8 b]> `0 16`
#>   ..$ :<nanoarrow_buffer data<string>[16 b]> `counterclockwise`
#>  $ dictionary: NULL
#>  $ children  : list()
```
