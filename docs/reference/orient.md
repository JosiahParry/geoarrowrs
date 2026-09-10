# Apply a winding direction to polygon rings

Rewinds each polygon so its exterior and interior rings follow a
consistent direction. The geometry type of the output matches the input.

## Usage

``` r
orient(geometry, direction = "default")
```

## Arguments

- geometry:

  a GeoArrow polygon or multipolygon array

- direction:

  one of `"default"`, `"reversed"`, `"ccw"`, or `"cw"`

## Value

a GeoArrow array of the same geometry type as `geometry`

## Details

`"default"` gives a counter-clockwise exterior ring and clockwise
interior rings, which is the winding the OGC simple features and GeoJSON
specifications call for. `"reversed"` gives the opposite. `"ccw"` and
`"cw"` are accepted as aliases and refer to the exterior ring.

Rewinding does not change which points a polygon covers, only the order
its coordinates are stored in. A null geometry stays null.

## References

[Orient](https://docs.rs/geo/latest/geo/algorithm/orient/trait.Orient.html)

## See also

Other winding:
[`is_ccw()`](https://josiahparry.github.io/geoarrowrs/reference/is_ccw.md),
[`winding_order()`](https://josiahparry.github.io/geoarrowrs/reference/winding_order.md)

## Examples

``` r
# a clockwise square
p <- sf::st_polygon(list(matrix(
  c(0, 0, 0, 1, 1, 1, 1, 0, 0, 0), ncol = 2, byrow = TRUE
)))
g <- geoarrow::as_geoarrow_array(sf::st_sfc(p))

winding_order(g)
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
winding_order(orient(g, "default"))
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
