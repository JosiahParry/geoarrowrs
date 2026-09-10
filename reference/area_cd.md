# Compute the signed and unsigned area using the Chamberlain-Duquette algorithm

These functions use the Chamberlain-Duquette formula, which is suitable
for geographic coordinates on a sphere. Results are in square meters.

## Usage

``` r
ga_signed_area_cd(x)

ga_unsigned_area_cd(x)
```

## Arguments

- x:

  a GeoArrow geometry array

## Value

a double vector of area values in square meters

## References

[ChamberlainDuquetteArea](https://docs.rs/geo/latest/geo/algorithm/chamberlain_duquette_area/trait.ChamberlainDuquetteArea.html)

[ChamberlainDuquetteArea](https://docs.rs/geo/latest/geo/algorithm/chamberlain_duquette_area/trait.ChamberlainDuquetteArea.html)

## See also

Other area:
[`ga_signed_area()`](https://josiahparry.github.io/geoarrowrs/reference/area.md),
[`ga_signed_area_geodesic()`](https://josiahparry.github.io/geoarrowrs/reference/area_geodesic.md)
