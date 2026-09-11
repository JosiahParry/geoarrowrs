# Convert coordinates from radians to degrees

Multiplies every x and y coordinate by `180 / pi`. The geometry type of
the output matches the geometry type of the input.

## Usage

``` r
ga_to_degrees(geometry)
```

## Arguments

- geometry:

  a GeoArrow geometry array

## Value

a GeoArrow array of the same geometry type as the input

## Details

Only the x and y coordinates are converted. Z and M values are dropped,
since the conversion goes through a two dimensional representation.

## References

[ToDegrees](https://docs.rs/geo/latest/geo/algorithm/convert_angle_unit/trait.ToDegrees.html)

## See also

Other convert:
[`ga_to_radians()`](https://josiahparry.github.io/geoarrowrs/reference/ga_to_radians.md)

## Examples

``` r
rad <- ga_xy(c(0, pi / 4), c(0, pi / 2))

geoarrow::as_geoarrow_vctr(ga_to_degrees(rad))
#> <geoarrow_vctr geoarrow.point{struct}[2]>
#> [1] <POINT (0 0)>   <POINT (45 90)>
```
