# Simplify with Ramer-Douglas-Peucker

Reduces the number of points in each geometry by removing vertices that
deviate less than `epsilon` from the simplified path. Accepts
linestrings, multilinestrings, polygons, and multipolygons.

## Usage

``` r
ga_simplify(geometry, epsilon)
```

## Arguments

- geometry:

  a GeoArrow linestring, multilinestring, polygon, or multipolygon array

- epsilon:

  the simplification tolerance; length 1 or the same length as
  `geometry`

## Value

a GeoArrow array of the same geometry type as the input

## References

[Simplify](https://docs.rs/geo/latest/geo/algorithm/simplify/trait.Simplify.html)

## See also

Other simplify:
[`ga_simplify_idx()`](https://josiahparry.github.io/geoarrowrs/reference/ga_simplify_idx.md),
[`ga_simplify_vw()`](https://josiahparry.github.io/geoarrowrs/reference/ga_simplify_vw.md)

## Examples

``` r
fp <- system.file("shape/nc.shp", package = "sf")
nc <- as.data.frame(read_shapefile(fp))

# a bigger epsilon drops more vertices
head(as.vector(ga_n_coords(nc$geometry)), 3)
#> [1] 27 26 28
head(as.vector(ga_n_coords(ga_simplify(nc$geometry, 0.01))), 3)
#> [1] 19 14 14
head(as.vector(ga_n_coords(ga_simplify(nc$geometry, 0.1))), 3)
#> [1] 5 5 5
```
