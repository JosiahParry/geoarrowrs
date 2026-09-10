# Compute the signed and unsigned planar area of geometries

`signed_area()` returns positive values for counter-clockwise winding
and negative values for clockwise winding. `unsigned_area()` always
returns a non-negative value.

## Usage

``` r
signed_area(x)

unsigned_area(x)
```

## Arguments

- x:

  a GeoArrow geometry array

## Value

a double vector of area values in the units of the coordinate system

## References

[Area](https://docs.rs/geo/latest/geo/algorithm/area/trait.Area.html)

[Area](https://docs.rs/geo/latest/geo/algorithm/area/trait.Area.html)

## See also

Other area:
[`signed_area_cd()`](https://josiahparry.github.io/geoarrowrs/reference/area_cd.md),
[`signed_area_geodesic()`](https://josiahparry.github.io/geoarrowrs/reference/area_geodesic.md)
