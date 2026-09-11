# Geodesic area and perimeter

These functions use the geodesic formula for computing area and
perimeter on an ellipsoidal model of the earth. Results are in square
meters for area and meters for perimeter.

## Usage

``` r
ga_signed_area_geodesic(x)

ga_unsigned_area_geodesic(x)

ga_perimeter_geodesic(x)
```

## Arguments

- x:

  a GeoArrow geometry array

## Value

a double vector of area values in square meters, or perimeter values in
meters

## Details

`geo` assumes Simple Features winding, so an exterior ring must run
counter-clockwise. A clockwise ring names the rest of the earth instead,
and `ga_unsigned_area_geodesic()` returns a value near 5.1e14.
Shapefiles wind clockwise, so orient with
[`ga_orient()`](https://josiahparry.github.io/geoarrowrs/reference/ga_orient.md)
first or read the sign off `ga_signed_area_geodesic()`, which is
unaffected.

## References

[GeodesicArea](https://docs.rs/geo/latest/geo/algorithm/geodesic_area/trait.GeodesicArea.html)

## See also

Other area:
[`ga_signed_area()`](https://josiahparry.github.io/geoarrowrs/reference/area.md),
[`ga_signed_area_cd()`](https://josiahparry.github.io/geoarrowrs/reference/area_cd.md)

## Examples

``` r
fp <- system.file("shape/nc.shp", package = "sf")
nc <- as.data.frame(read_shapefile(fp))

head(as.vector(ga_signed_area_geodesic(nc$geometry)))
#> [1] -1137389166  -611077451 -1423490699  -694546681 -1520741303  -967728604
head(as.vector(ga_perimeter_geodesic(nc$geometry)))
#> [1] 141665.2 119928.6 160497.5 301517.1 211952.3 160891.0

# nc winds clockwise, so orient before taking the unsigned area
ccw <- ga_orient(nc$geometry, "default")
head(as.vector(ga_unsigned_area_geodesic(ccw)))
#> [1] 1137389166  611077451 1423490699  694546681 1520741303  967728604
```
