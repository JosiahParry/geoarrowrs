# Compute the signed and unsigned geodesic area and perimeter of geometries

These functions use the geodesic formula for computing area and
perimeter on an ellipsoidal model of the earth. Results are in square
meters for area and meters for perimeter.

## Usage

``` r
signed_area_geodesic(x)

unsigned_area_geodesic(x)

perimeter_signed_geodesic(x)

perimeter_unsigned_geodesic(x)
```

## Arguments

- x:

  a GeoArrow geometry array

## Value

a double vector of area values in square meters, or perimeter values in
meters

## References

[GeodesicArea](https://docs.rs/geo/latest/geo/algorithm/geodesic_area/trait.GeodesicArea.html)

[GeodesicArea](https://docs.rs/geo/latest/geo/algorithm/geodesic_area/trait.GeodesicArea.html)

[GeodesicArea](https://docs.rs/geo/latest/geo/algorithm/geodesic_area/trait.GeodesicArea.html)

[GeodesicArea](https://docs.rs/geo/latest/geo/algorithm/geodesic_area/trait.GeodesicArea.html)

## See also

Other area:
[`signed_area()`](https://josiahparry.github.io/geoarrowrs/reference/area.md),
[`signed_area_cd()`](https://josiahparry.github.io/geoarrowrs/reference/area_cd.md)
