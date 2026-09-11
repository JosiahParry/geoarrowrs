# Test whether geometries are well formed

`TRUE` when a geometry satisfies the OGC simple features rules.

Returns the first validation problem found, or `NA` when the geometry is
valid.

## Usage

``` r
ga_is_valid(geometry)

ga_validation_error(geometry)
```

## Arguments

- geometry:

  a GeoArrow geometry array

## Value

a boolean array of the same length as `geometry`

a string array of the same length as `geometry`

## Details

Several algorithms give wrong answers on invalid input rather than
failing, so this is worth checking before trusting a result. Common
problems are a polygon whose rings self intersect, a ring with too few
points, and a coordinate that is `NaN` or infinite.

A null geometry gives `NA`. Use `ga_validation_error()` to see why a
geometry failed.

A geometry can break more than one rule; only the first is reported,
since fixing it often resolves the rest.

## References

[Validation](https://docs.rs/geo/latest/geo/algorithm/validation/trait.Validation.html)

## Examples

``` r
square <- rbind(c(0, 0), c(2, 0), c(2, 2), c(0, 2), c(0, 0))
bowtie <- rbind(c(0, 0), c(2, 2), c(2, 0), c(0, 2), c(0, 0))
good <- sf::st_polygon(list(square))
bad <- sf::st_polygon(list(bowtie))
g <- geoarrow::as_geoarrow_array(sf::st_sfc(good, bad))

as.vector(ga_is_valid(g))
#> [1]  TRUE FALSE
as.vector(ga_validation_error(g))
#> [1] NA                                     
#> [2] "exterior ring has a self-intersection"
```
