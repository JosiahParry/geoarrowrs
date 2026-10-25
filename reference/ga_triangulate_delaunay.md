# Delaunay triangulation

Returns one multipolygon per input geometry, whose parts are that
geometry's triangles. `ga_triangulate_delaunay()` preserves the input's
length.

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

  the distance below which to snap coordinates to the same position;
  length 1 or the same length as `x`. `geo` uses 1e-4 by default

## Value

a GeoArrow multipolygon array of the same length as `x`

## Details

With `constrained = TRUE`, `ga_triangulate_delaunay()` keeps the input's
edges and returns only the triangles that fall inside the geometry. With
`constrained = FALSE`, it triangulates the convex hull of the input's
vertices, ignoring the edges, so the triangles may cross holes and
concavities.

It returns a null element for a null geometry or one it cannot
triangulate.

## References

[TriangulateDelaunay](https://docs.rs/geo/latest/geo/algorithm/triangulate_delaunay/trait.TriangulateDelaunay.html)

## See also

Other triangulate:
[`ga_triangulate_earcut()`](https://josiahparry.github.io/geoarrowrs/reference/ga_triangulate_earcut.md)

## Examples

``` r
fp <- system.file("shape/nc.shp", package = "sf")
nc <- as.data.frame(read_shapefile(fp))

# with constrained = TRUE you get only triangles inside the county boundary
tri <- ga_triangulate_delaunay(nc$geometry)
tri$length
#> [1] 100

# set constrained = FALSE to fill the convex hull instead, which covers more
hull <- ga_triangulate_delaunay(nc$geometry, constrained = FALSE)
head(as.vector(ga_unsigned_area(tri)), 3)
#> [1] 0.11428350 0.06139976 0.14301628
head(as.vector(ga_unsigned_area(hull)), 3)
#> [1] 0.12484563 0.07437819 0.15586877
```
