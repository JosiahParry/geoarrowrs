# Signed and unsigned planar area

`ga_signed_area()` returns positive values for counter-clockwise winding
and negative values for clockwise winding. `ga_unsigned_area()` always
returns a non-negative value.

## Usage

``` r
ga_signed_area(x)

ga_unsigned_area(x)
```

## Arguments

- x:

  a GeoArrow geometry array

## Value

a double vector of area values in the units of the coordinate system

## References

[Area](https://docs.rs/geo/latest/geo/algorithm/area/trait.Area.html)

## See also

Other area:
[`ga_signed_area_cd()`](https://josiahparry.github.io/geoarrowrs/reference/area_cd.md),
[`ga_signed_area_geodesic()`](https://josiahparry.github.io/geoarrowrs/reference/area_geodesic.md)

## Examples

``` r
fp <- system.file("shape/nc.shp", package = "sf")
nc <- as.data.frame(read_shapefile(fp))

# nc rings wind clockwise, so the signed area is negative
head(as.vector(ga_signed_area(nc$geometry)))
#> [1] -0.11428350 -0.06139976 -0.14301628 -0.06977098 -0.15275930 -0.09715756
head(as.vector(ga_unsigned_area(nc$geometry)))
#> [1] 0.11428350 0.06139976 0.14301628 0.06977098 0.15275930 0.09715756
```
