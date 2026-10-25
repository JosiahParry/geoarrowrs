# Compute the extreme coordinates of geometries

Returns a struct array with four point fields, `x_min`, `x_max`, `y_min`
and `y_max`, holding the coordinate that is furthest in each direction.
`ga_extremes()` returns one row per input geometry, and a row of four
nulls for a null or empty geometry.

## Usage

``` r
ga_extremes(x)
```

## Arguments

- x:

  a GeoArrow geometry array

## Value

an Arrow struct array of four point fields, with one row per geometry

## Details

Note these are the extreme *coordinates*, not the corners of the
bounding box: `ga_extremes()` gives the `x_min` point the y value of
whichever vertex was leftmost. Use
[`ga_envelope()`](https://josiahparry.github.io/geoarrowrs/reference/ga_envelope.md)
for the bounding box.

## References

[Extremes](https://docs.rs/geo/latest/geo/algorithm/extremes/trait.Extremes.html)

## See also

Other boundary:
[`ga_concave_hull()`](https://josiahparry.github.io/geoarrowrs/reference/ga_concave_hull.md),
[`ga_convex_hull()`](https://josiahparry.github.io/geoarrowrs/reference/ga_convex_hull.md),
[`ga_envelope()`](https://josiahparry.github.io/geoarrowrs/reference/ga_envelope.md),
[`ga_minimum_rotated_rect()`](https://josiahparry.github.io/geoarrowrs/reference/ga_minimum_rotated_rect.md)

## Examples

``` r
fp <- system.file("shape/nc.shp", package = "sf")
nc <- as.data.frame(read_shapefile(fp))

# one row per county, four point columns
head(as.data.frame(ga_extremes(nc$geometry)), 3)
#>                              x_min                            x_max
#> 1 <POINT (-81.7410736 36.3917847)> <POINT (-81.2398911 36.3653641)>
#> 2 <POINT (-81.3475418 36.5379143)> <POINT (-80.9034424 36.5652122)>
#> 3 <POINT (-80.9657745 36.4672203)> <POINT (-80.4353104 36.5510445)>
#>                              y_min                            y_max
#> 1 <POINT (-81.4727554 36.2343559)> <POINT (-81.6699982 36.5896492)>
#> 2 <POINT (-81.2398911 36.3653641)> <POINT (-81.3452988 36.5728645)>
#> 3  <POINT (-80.874382 36.2338829)> <POINT (-80.9034424 36.5652122)>
```
