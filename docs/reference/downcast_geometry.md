# Cast geometries to the narrowest type that fits them

Inspects the geometries and casts to the most specific type that can
hold every one of them. A `geometry` array holding only points becomes a
`point` array; one holding points and polygons is left alone, since no
narrower type fits.

## Usage

``` r
downcast_geometry(x)
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
[`cast_geometry()`](https://josiahparry.github.io/geoarrowrs/reference/cast_geometry.md),
[`explode()`](https://josiahparry.github.io/geoarrowrs/reference/explode.md),
[`flatten()`](https://josiahparry.github.io/geoarrowrs/reference/flatten.md)
