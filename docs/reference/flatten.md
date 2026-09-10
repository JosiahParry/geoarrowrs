# Collapse a list of geometries into a single array

The inverse of
[`explode()`](https://josiahparry.github.io/geoarrowrs/reference/explode.md).
Concatenates every element's parts into one array, so a list of `n`
elements holding `m` parts in total becomes an array of length `m`.

## Usage

``` r
flatten(x)
```

## Arguments

- x:

  a list array of GeoArrow arrays, as returned by
  [`explode()`](https://josiahparry.github.io/geoarrowrs/reference/explode.md)

## Value

a GeoArrow array holding every part, flattened

## Details

Null elements contribute nothing, so the result is shorter than the
input whenever an element holds more or fewer than one geometry.

## See also

Other cast:
[`cast_geometry()`](https://josiahparry.github.io/geoarrowrs/reference/cast_geometry.md),
[`downcast_geometry()`](https://josiahparry.github.io/geoarrowrs/reference/downcast_geometry.md),
[`explode()`](https://josiahparry.github.io/geoarrowrs/reference/explode.md)
