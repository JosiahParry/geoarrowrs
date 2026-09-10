# Simplify geometries using the Visvalingam-Whyatt algorithm

Reduces the number of points in each geometry by removing vertices whose
effective area is below `epsilon`. Accepts linestrings,
multilinestrings, polygons, and multipolygons.

Like `ga_simplify_vw()` but preserves topology by preventing
self-intersections during simplification. Accepts linestrings,
multilinestrings, polygons, and multipolygons.

## Usage

``` r
ga_simplify_vw(geometry, epsilon)

ga_simplify_vw_preserve(geometry, epsilon)
```

## Arguments

- geometry:

  a GeoArrow linestring, multilinestring, polygon, or multipolygon array

- epsilon:

  the simplification tolerance; length 1 or the same length as
  `geometry`

## Value

a GeoArrow array of the same geometry type as the input

a GeoArrow array of the same geometry type as the input

## References

[SimplifyVw](https://docs.rs/geo/latest/geo/algorithm/simplify_vw/trait.SimplifyVw.html)

[SimplifyVwPreserve](https://docs.rs/geo/latest/geo/algorithm/simplify_vw/trait.SimplifyVwPreserve.html)

## See also

Other simplify:
[`ga_simplify()`](https://josiahparry.github.io/geoarrowrs/reference/ga_simplify.md),
[`ga_simplify_idx()`](https://josiahparry.github.io/geoarrowrs/reference/ga_simplify_idx.md)
