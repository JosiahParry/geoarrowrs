# Convert coordinates from radians to degrees

Multiplies every x and y coordinate by `180 / pi`. The geometry type of
the output matches the geometry type of the input.

## Usage

``` r
to_degrees(geometry)
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
[`to_radians()`](https://josiahparry.github.io/geoarrowrs/reference/to_radians.md)
