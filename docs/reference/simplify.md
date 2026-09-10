# Simplify geometries using the Ramer-Douglas-Peucker algorithm

Reduces the number of points in each geometry by removing vertices that
deviate less than `epsilon` from the simplified path. Accepts
linestrings, multilinestrings, polygons, and multipolygons.

## Usage

``` r
simplify(geometry, epsilon)
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
[`simplify_vw()`](https://josiahparry.github.io/geoarrowrs/reference/simplify_vw.md)
