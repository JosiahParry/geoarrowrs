# Triangulate geometries with a Delaunay triangulation

Returns one multipolygon per input geometry, whose parts are that
geometry's triangles. The output has the same length as the input.

## Usage

``` r
ga_triangulate_delaunay(x, constrained = TRUE, snap_radius = 1e-04)
```

## Arguments

- x:

  a GeoArrow geometry array

- constrained:

  whether to constrain the triangulation to the input's edges. `TRUE` by
  default

- snap_radius:

  coordinates closer together than this are snapped to the same
  position; length 1 or the same length as `x`. `geo` uses 1e-4 by
  default

## Value

a GeoArrow multipolygon array of the same length as `x`

## Details

A constrained triangulation keeps the input's edges and returns only the
triangles that fall inside the geometry. An unconstrained triangulation
triangulates the convex hull of the input's vertices, ignoring its
edges, so it may cross holes and concavities.

A null geometry, or one that cannot be triangulated, becomes a null
element.

## References

[TriangulateDelaunay](https://docs.rs/geo/latest/geo/algorithm/triangulate_delaunay/trait.TriangulateDelaunay.html)

## See also

Other triangulate:
[`ga_triangulate_earcut()`](https://josiahparry.github.io/geoarrowrs/reference/ga_triangulate_earcut.md)
