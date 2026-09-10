# Read an ESRI Shapefile into a record batch stream

Reads the `.shp` geometries together with the `.dbf` attributes. The CRS
is taken from the sibling `.prj` file when one is present.

## Usage

``` r
read_shapefile(path)
```

## Arguments

- path:

  path to a `.shp` file. The sibling `.dbf` and `.prj` files are
  resolved from the same base name.

## Value

a `nanoarrow_array_stream` of a single record batch, holding the `.dbf`
attribute columns followed by a `geometry` column.

## Details

Polylines are read as multilinestrings and polygons as multipolygons,
because a shapefile record of either type may contain multiple parts.

The coordinate dimension is chosen once per file, from the shape type
and from whether the coordinates actually carry measures. A `PolygonZ`
file with no measures is therefore read as XYZ, rather than as XYZM full
of nulls.

|  |  |  |
|----|----|----|
| shape type | measures present | dimension |
| `Point`, `Polyline`, `Polygon`, `Multipoint` | not applicable | XY |
| `PointM`, `PolylineM`, `PolygonM`, `MultipointM` | no | XY |
| `PointM`, `PolylineM`, `PolygonM`, `MultipointM` | yes | XYM |
| `PointZ`, `PolylineZ`, `PolygonZ`, `MultipointZ` | no | XYZ |
| `PointZ`, `PolylineZ`, `PolygonZ`, `MultipointZ` | yes | XYZM |

If only some coordinates carry a measure, M is dropped and a message is
emitted, because an Arrow column cannot mix dimensions.

`Multipatch` files are not supported.

## See also

Other io:
[`read_flatgeobuf()`](https://josiahparry.github.io/geoarrowrs/reference/read_flatgeobuf.md),
[`read_geojson()`](https://josiahparry.github.io/geoarrowrs/reference/read_geojson.md)

## Examples

``` r
path <- system.file("shape/nc.shp", package = "sf")
df <- as.data.frame(read_shapefile(path))

# the .dbf attribute columns, followed by `geometry`
dim(df)
#> [1] 100  15
head(df[, c("NAME", "BIR74", "geometry")], 3)
#>        NAME BIR74
#> 1      Ashe  1091
#> 2 Alleghany   487
#> 3     Surry  3188
#>                                                                   geometry
#> 1 <MULTIPOLYGON (((-81.4727554 36.2343559, -81.5408401 36.2725067, -81.56>
#> 2 <MULTIPOLYGON (((-81.2398911 36.3653641, -81.2406921 36.3794174, -81.26>
#> 3 <MULTIPOLYGON (((-80.4563446 36.2425575, -80.476387 36.2547264, -80.536>
```
