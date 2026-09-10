# Split multi-part geometries into their parts

Returns a list the same length as the input, where each element is an
array of that geometry's constituent parts. A multipolygon of three
polygons becomes one element holding a polygon array of length three.

## Usage

``` r
explode(x)
```

## Arguments

- x:

  a GeoArrow geometry array

## Value

a list array of the same length as `x`, whose elements are GeoArrow
arrays

## Details

The output geometry type is the singular form of the input:

|                                 |              |
|---------------------------------|--------------|
| input                           | element type |
| `multipoint`, `point`           | `point`      |
| `multilinestring`, `linestring` | `linestring` |
| `multipolygon`, `polygon`       | `polygon`    |

The element type is decided by the array's declared type, not by its
contents, so an array of multipolygons that all happen to hold one part
still explodes to polygons. A mixed `geometry` array has no single
singular type and errors; narrow it with
[`downcast_geometry()`](https://josiahparry.github.io/geoarrowrs/reference/downcast_geometry.md)
first.

A singular geometry yields an element of length one, so the operation is
well defined for any input. A null geometry yields a null element, which
is distinct from an empty one.

Because the length is preserved, the result lines up with the row it
came from and can sit alongside the other columns of a table. Use
[`flatten()`](https://josiahparry.github.io/geoarrowrs/reference/flatten.md)
to collapse it into a single array with one row per part.

## See also

Other cast:
[`cast_geometry()`](https://josiahparry.github.io/geoarrowrs/reference/cast_geometry.md),
[`downcast_geometry()`](https://josiahparry.github.io/geoarrowrs/reference/downcast_geometry.md),
[`flatten()`](https://josiahparry.github.io/geoarrowrs/reference/flatten.md)
