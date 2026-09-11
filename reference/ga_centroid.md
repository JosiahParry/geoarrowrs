# Compute the centroid of geometries

Returns the centroid point of each geometry. Returns `NA` for empty
geometries.

## Usage

``` r
ga_centroid(x)
```

## Arguments

- x:

  a GeoArrow geometry array

## Value

a GeoArrow point array

## References

[Centroid](https://docs.rs/geo/latest/geo/algorithm/centroid/trait.Centroid.html)

## See also

Other misc:
[`ga_buffer()`](https://josiahparry.github.io/geoarrowrs/reference/ga_buffer.md),
[`ga_chaikin_smoothing()`](https://josiahparry.github.io/geoarrowrs/reference/ga_chaikin_smoothing.md),
[`ga_line_segmentize()`](https://josiahparry.github.io/geoarrowrs/reference/ga_line_segmentize.md),
[`ga_remove_repeated_points()`](https://josiahparry.github.io/geoarrowrs/reference/ga_remove_repeated_points.md)

## Examples

``` r
fp <- system.file("shape/nc.shp", package = "sf")
nc <- as.data.frame(read_shapefile(fp))

head(geoarrow::as_geoarrow_vctr(ga_centroid(nc$geometry)), 3)
#> <geoarrow_vctr geoarrow.point{struct}[3]>
#> [1] <POINT (-81.4982613 36.4313986)> <POINT (-81.1251451 36.4910109)>
#> [3] <POINT (-80.6857466 36.4125214)>
```
