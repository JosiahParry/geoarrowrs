# Triangulate polygons with the earcut algorithm

Returns one multipolygon per input geometry, whose parts are that
geometry's triangles. The output has the same length as the input, so a
triangulated geometry stays aligned with the row it came from.

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

Earcut is defined for polygons only. A multipolygon is triangulated part
by part and the triangles are merged into a single multipolygon. Any
other geometry type, a null geometry, or a polygon that cannot be
triangulated becomes a null element.

Earcut is fast and respects interior rings, but the triangles it
produces are not Delaunay. Use
[`ga_triangulate_delaunay()`](https://josiahparry.github.io/geoarrowrs/reference/ga_triangulate_delaunay.md)
when triangle quality matters.

## References

[TriangulateEarcut](https://docs.rs/geo/latest/geo/algorithm/triangulate_earcut/trait.TriangulateEarcut.html)

## See also

Other triangulate:
[`ga_triangulate_delaunay()`](https://josiahparry.github.io/geoarrowrs/reference/ga_triangulate_delaunay.md)
