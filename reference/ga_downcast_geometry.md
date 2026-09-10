# Cast geometries to the narrowest type that fits them

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
