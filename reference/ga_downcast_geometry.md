# Cast to the narrowest geometry type

Inspects the geometries and casts to the most specific type that can
hold every one of them. A `geometry` array holding only points becomes a
`point` array; one holding points and polygons is left alone, since no
narrower type fits.

## Usage

``` r
ga_downcast_geometry(x)
```

## Arguments

- x:

  a GeoArrow geometry array

## Value

a GeoArrow array of the narrowest type that fits, the same length as `x`

## Details

This is the inverse of casting up to `geometry`, and is useful after
reading a format that does not record a single geometry type.

## See also

Other cast:
[`ga_as_point()`](https://josiahparry.github.io/geoarrowrs/reference/ga_as_point.md),
[`ga_cast_geometry()`](https://josiahparry.github.io/geoarrowrs/reference/ga_cast_geometry.md),
[`ga_explode()`](https://josiahparry.github.io/geoarrowrs/reference/ga_explode.md),
[`ga_flatten()`](https://josiahparry.github.io/geoarrowrs/reference/ga_flatten.md),
[`ga_from_wkb()`](https://josiahparry.github.io/geoarrowrs/reference/ga_from_wkb.md)

## Examples

``` r
pts <- ga_xy(c(0, 1), c(0, 1))
wide <- ga_cast_geometry(pts, "geometry")

# only points in there, so it narrows back to a point array
ga_downcast_geometry(wide)
#> <nanoarrow_array geoarrow.point{struct}[2]>
#>  $ length    : int 2
#>  $ null_count: int 0
#>  $ offset    : int 0
#>  $ buffers   :List of 1
#>   ..$ :<nanoarrow_buffer validity<bool>[null] ``
#>  $ children  :List of 2
#>   ..$ x:<nanoarrow_array double[2]>
#>   .. ..$ length    : int 2
#>   .. ..$ null_count: int 0
#>   .. ..$ offset    : int 0
#>   .. ..$ buffers   :List of 2
#>   .. .. ..$ :<nanoarrow_buffer validity<bool>[null] ``
#>   .. .. ..$ :<nanoarrow_buffer data<double>[2][16 b]> `0 1`
#>   .. ..$ dictionary: NULL
#>   .. ..$ children  : list()
#>   ..$ y:<nanoarrow_array double[2]>
#>   .. ..$ length    : int 2
#>   .. ..$ null_count: int 0
#>   .. ..$ offset    : int 0
#>   .. ..$ buffers   :List of 2
#>   .. .. ..$ :<nanoarrow_buffer validity<bool>[null] ``
#>   .. .. ..$ :<nanoarrow_buffer data<double>[2][16 b]> `0 1`
#>   .. ..$ dictionary: NULL
#>   .. ..$ children  : list()
#>  $ dictionary: NULL
```
