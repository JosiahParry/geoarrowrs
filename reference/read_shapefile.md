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

# read into a nanoarrow array stream
res <- read_shapefile(path)
res
#> <nanoarrow_array_stream struct<AREA: double, PERIMETER: double, CNTY_: double, CNTY_ID: double, NAME: string, FIPS: string, FIPSNO: double, CRESS_ID: double, BIR74: double, SID74: double, NWBIR74: double, BIR79: double, SID79: double, NWBIR79: double, geometry: geoarrow.multipolygon{list<polygons: list<rings: list<vertices: struct<x: double, y: double>>>>}>>
#>  $ get_schema:function ()  
#>  $ get_next  :function (schema = x$get_schema(), validate = TRUE)  
#>  $ release   :function ()  

# convert to a df
df <- as.data.frame(res)
head(df)
#>    AREA PERIMETER CNTY_ CNTY_ID        NAME  FIPS FIPSNO CRESS_ID BIR74 SID74
#> 1 0.114     1.442  1825    1825        Ashe 37009  37009        5  1091     1
#> 2 0.061     1.231  1827    1827   Alleghany 37005  37005        3   487     0
#> 3 0.143     1.630  1828    1828       Surry 37171  37171       86  3188     5
#> 4 0.070     2.968  1831    1831   Currituck 37053  37053       27   508     1
#> 5 0.153     2.206  1832    1832 Northampton 37131  37131       66  1421     9
#> 6 0.097     1.670  1833    1833    Hertford 37091  37091       46  1452     7
#>   NWBIR74 BIR79 SID79 NWBIR79
#> 1      10  1364     0      19
#> 2      10   542     3      12
#> 3     208  3616     6     260
#> 4     123   830     2     145
#> 5    1066  1606     3    1197
#> 6     954  1838     5    1237
#>                                                                   geometry
#> 1 <MULTIPOLYGON (((-81.4727554 36.2343559, -81.5408401 36.2725067, -81.56>
#> 2 <MULTIPOLYGON (((-81.2398911 36.3653641, -81.2406921 36.3794174, -81.26>
#> 3 <MULTIPOLYGON (((-80.4563446 36.2425575, -80.476387 36.2547264, -80.536>
#> 4 <MULTIPOLYGON (((-76.0089722 36.3195953, -76.0173492 36.3377304, -76.03>
#> 5 <MULTIPOLYGON (((-77.2176666 36.2409821, -77.2346115 36.2145996, -77.29>
#> 6 <MULTIPOLYGON (((-76.7450638 36.2339172, -76.98069 36.2302361, -76.9947>
```
