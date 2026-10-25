# Convert coordinates from degrees to radians

Multiplies every x and y coordinate by `pi / 180`. `ga_to_radians()`
keeps the geometry type of the input.

## Usage

``` r
ga_to_radians(geometry)
```

## Arguments

- geometry:

  a GeoArrow geometry array

## Value

a GeoArrow array of the same geometry type as the input

## Details

`ga_to_radians()` converts only the x and y coordinates. It drops Z and
M values, since it goes through a two dimensional representation.

## References

[ToRadians](https://docs.rs/geo/latest/geo/algorithm/convert_angle_unit/trait.ToRadians.html)

## See also

Other convert:
[`ga_to_degrees()`](https://josiahparry.github.io/geoarrowrs/reference/ga_to_degrees.md)

## Examples

``` r
deg <- ga_point(c(0, 45), c(0, 90))

geoarrow::as_geoarrow_vctr(ga_to_radians(deg))
#> <geoarrow_vctr geoarrow.point{struct}[2]>
#> [1] <POINT (0 0)>                 <POINT (0.7853982 1.5707963)>
```
