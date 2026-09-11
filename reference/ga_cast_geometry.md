# Cast geometries to another GeoArrow geometry type

Changes the geometry type of an array without changing the geometries it
holds.

## Usage

``` r
ga_cast_geometry(x, to)
```

## Arguments

- x:

  a GeoArrow geometry array

- to:

  the target type, one of `"point"`, `"linestring"`, `"polygon"`,
  `"multipoint"`, `"multilinestring"`, `"multipolygon"`,
  `"geometrycollection"`, `"geometry"`, `"wkb"`, or `"wkt"`

## Value

a GeoArrow array of the requested type, the same length as `x`

## Details

Some casts always succeed:

|              |                          |
|--------------|--------------------------|
| from         | to                       |
| `point`      | `multipoint`             |
| `linestring` | `multilinestring`        |
| `polygon`    | `multipolygon`           |
| any type     | `geometry`, `wkb`, `wkt` |

Others are fallible and error when a geometry does not fit the target
type, such as a `multipoint` holding two points cast to `point`:

|                   |                   |
|-------------------|-------------------|
| from              | to                |
| `multipoint`      | `point`           |
| `multilinestring` | `linestring`      |
| `multipolygon`    | `polygon`         |
| `geometry`        | any concrete type |

The dimension is carried over from the input, and casting between
different dimensions is not supported.

## See also

Other cast:
[`ga_as_point()`](https://josiahparry.github.io/geoarrowrs/reference/ga_as_point.md),
[`ga_downcast_geometry()`](https://josiahparry.github.io/geoarrowrs/reference/ga_downcast_geometry.md),
[`ga_explode()`](https://josiahparry.github.io/geoarrowrs/reference/ga_explode.md),
[`ga_flatten()`](https://josiahparry.github.io/geoarrowrs/reference/ga_flatten.md),
[`ga_from_wkb()`](https://josiahparry.github.io/geoarrowrs/reference/ga_from_wkb.md)

## Examples

``` r
pts <- ga_xy(c(0, 1), c(0, 1))

# widening always works
ga_cast_geometry(pts, "multipoint")
#> <nanoarrow_array geoarrow.multipoint{list}[2]>
#>  $ length    : int 2
#>  $ null_count: int 0
#>  $ offset    : int 0
#>  $ buffers   :List of 2
#>   ..$ :<nanoarrow_buffer validity<bool>[null] ``
#>   ..$ :<nanoarrow_buffer data_offset<int32>[3][12 b]> `0 1 2`
#>  $ children  :List of 1
#>   ..$ points:<nanoarrow_array struct[2]>
#>   .. ..$ length    : int 2
#>   .. ..$ null_count: int 0
#>   .. ..$ offset    : int 0
#>   .. ..$ buffers   :List of 1
#>   .. .. ..$ :<nanoarrow_buffer validity<bool>[null] ``
#>   .. ..$ children  :List of 2
#>   .. .. ..$ x:<nanoarrow_array double[2]>
#>   .. .. .. ..$ length    : int 2
#>   .. .. .. ..$ null_count: int 0
#>   .. .. .. ..$ offset    : int 0
#>   .. .. .. ..$ buffers   :List of 2
#>   .. .. .. .. ..$ :<nanoarrow_buffer validity<bool>[null] ``
#>   .. .. .. .. ..$ :<nanoarrow_buffer data<double>[2][16 b]> `0 1`
#>   .. .. .. ..$ dictionary: NULL
#>   .. .. .. ..$ children  : list()
#>   .. .. ..$ y:<nanoarrow_array double[2]>
#>   .. .. .. ..$ length    : int 2
#>   .. .. .. ..$ null_count: int 0
#>   .. .. .. ..$ offset    : int 0
#>   .. .. .. ..$ buffers   :List of 2
#>   .. .. .. .. ..$ :<nanoarrow_buffer validity<bool>[null] ``
#>   .. .. .. .. ..$ :<nanoarrow_buffer data<double>[2][16 b]> `0 1`
#>   .. .. .. ..$ dictionary: NULL
#>   .. .. .. ..$ children  : list()
#>   .. ..$ dictionary: NULL
#>  $ dictionary: NULL

# so does going out to wkb, which is what plain Parquet stores
ga_cast_geometry(pts, "wkb")
#> <nanoarrow_array geoarrow.wkb{binary}[2]>
#>  $ length    : int 2
#>  $ null_count: int 0
#>  $ offset    : int 0
#>  $ buffers   :List of 3
#>   ..$ :<nanoarrow_buffer validity<bool>[null] ``
#>   ..$ :<nanoarrow_buffer data_offset<int32>[3][12 b]> `0 21 42`
#>   ..$ :<nanoarrow_buffer data<binary>[42 b]> `01 01 00 00 00 00 00 00 00 00 ...`
#>  $ dictionary: NULL
#>  $ children  : list()
```
