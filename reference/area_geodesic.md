# Compute the signed and unsigned geodesic area and perimeter of geometries

These functions use the geodesic formula for computing area and
perimeter on an ellipsoidal model of the earth. Results are in square
meters for area and meters for perimeter.

## Usage

``` r
ga_signed_area_geodesic(x)

ga_unsigned_area_geodesic(x)

ga_perimeter_signed_geodesic(x)

ga_perimeter_unsigned_geodesic(x)
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
[`ga_signed_area()`](https://josiahparry.github.io/geoarrowrs/reference/area.md),
[`ga_signed_area_cd()`](https://josiahparry.github.io/geoarrowrs/reference/area_cd.md)
