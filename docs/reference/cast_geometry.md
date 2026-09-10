# Cast geometries to another GeoArrow geometry type

Changes the geometry type of an array without changing the geometries it
holds.

## Usage

``` r
cast_geometry(x, to)
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
[`downcast_geometry()`](https://josiahparry.github.io/geoarrowrs/reference/downcast_geometry.md),
[`explode()`](https://josiahparry.github.io/geoarrowrs/reference/explode.md),
[`flatten()`](https://josiahparry.github.io/geoarrowrs/reference/flatten.md)
