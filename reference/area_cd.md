# Chamberlain-Duquette spherical area

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

## See also

Other area:
[`ga_signed_area()`](https://josiahparry.github.io/geoarrowrs/reference/area.md),
[`ga_signed_area_geodesic()`](https://josiahparry.github.io/geoarrowrs/reference/area_geodesic.md)

## Examples

``` r
fp <- system.file("shape/nc.shp", package = "sf")
nc <- as.data.frame(read_shapefile(fp))

# square meters, not square degrees
head(as.vector(ga_unsigned_area_cd(nc$geometry)))
#> [1] 1139433024  611704011 1426267143  695828090 1523251541  969518599
head(as.vector(ga_signed_area_cd(nc$geometry)))
#> [1] -1139433024  -611704011 -1426267143  -695828090 -1523251541  -969518599
```
