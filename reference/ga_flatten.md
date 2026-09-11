# Collapse a list of geometries into a single array

The inverse of
[`ga_explode()`](https://josiahparry.github.io/geoarrowrs/reference/ga_explode.md).
Concatenates every element's parts into one array, so a list of `n`
elements holding `m` parts in total becomes an array of length `m`.

## Usage

``` r
ga_flatten(x)
```

## Arguments

- x:

  a list array of GeoArrow arrays, as returned by
  [`ga_explode()`](https://josiahparry.github.io/geoarrowrs/reference/ga_explode.md)

## Value

a GeoArrow array holding every part, flattened

## Details

Null elements contribute nothing, so the result is shorter than the
input whenever an element holds more or fewer than one geometry.

## See also

Other cast:
[`ga_as_point()`](https://josiahparry.github.io/geoarrowrs/reference/ga_as_point.md),
[`ga_cast_geometry()`](https://josiahparry.github.io/geoarrowrs/reference/ga_cast_geometry.md),
[`ga_downcast_geometry()`](https://josiahparry.github.io/geoarrowrs/reference/ga_downcast_geometry.md),
[`ga_explode()`](https://josiahparry.github.io/geoarrowrs/reference/ga_explode.md),
[`ga_from_wkb()`](https://josiahparry.github.io/geoarrowrs/reference/ga_from_wkb.md)

## Examples

``` r
fp <- system.file("shape/nc.shp", package = "sf")
nc <- as.data.frame(read_shapefile(fp))

# 100 counties in, one row per polygon part out
length(nc$geometry)
#> [1] 100
ga_flatten(ga_explode(nc$geometry))$length
#> [1] 108
```
