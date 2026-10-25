# Triangulate polygons with the earcut algorithm

Returns one multipolygon per input geometry, whose parts are that
geometry's triangles. `ga_triangulate_earcut()` preserves the input's
length, so it keeps each triangulated geometry aligned with the row it
came from.

## Usage

``` r
ga_triangulate_earcut(x)
```

## Arguments

- x:

  a GeoArrow polygon or multipolygon array

## Value

a GeoArrow multipolygon array of the same length as `x`

## Details

Earcut handles polygons only. `ga_triangulate_earcut()` triangulates a
multipolygon part by part and merges the triangles into a single
multipolygon. It returns a null element for any other geometry type, a
null geometry, or a polygon it cannot triangulate.

Earcut is fast and respects interior rings, but the triangles it
produces are not Delaunay. Use
[`ga_triangulate_delaunay()`](https://josiahparry.github.io/geoarrowrs/reference/ga_triangulate_delaunay.md)
when triangle quality matters.

## References

[TriangulateEarcut](https://docs.rs/geo/latest/geo/algorithm/triangulate_earcut/trait.TriangulateEarcut.html)

## See also

Other triangulate:
[`ga_triangulate_delaunay()`](https://josiahparry.github.io/geoarrowrs/reference/ga_triangulate_delaunay.md)

## Examples

``` r
fp <- system.file("shape/nc.shp", package = "sf")
nc <- as.data.frame(read_shapefile(fp))

# one multipolygon of triangles per county, so you keep the input's length
tri <- ga_triangulate_earcut(nc$geometry)
tri$length
#> [1] 100
head(as.vector(ga_n_coords(tri)), 3)
#> [1]  96  92 100
```
