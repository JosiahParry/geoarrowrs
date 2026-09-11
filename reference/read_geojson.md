# Read a GeoJSON FeatureCollection

Reads the geometries together with the feature properties. Properties
are untyped in GeoJSON, so each column's arrow type is inferred by
scanning every feature.

## Usage

``` r
read_geojson(path)
```

## Arguments

- path:

  path to a `.geojson` file holding a `FeatureCollection`.

## Value

a `nanoarrow_array_stream` of a single record batch, holding the
property columns followed by a `geometry` column.

## Details

The geometry column is narrowed to the most specific type that holds
every feature, promoting a singular type to its multi form where the two
are mixed. A feature with a `null` geometry becomes a null element and
does not affect the choice:

|                                         |                         |
|-----------------------------------------|-------------------------|
| geometry types in the file              | column type             |
| all points                              | `geoarrow.point`        |
| points and multipoints                  | `geoarrow.multipoint`   |
| all polygons                            | `geoarrow.polygon`      |
| polygons and multipolygons              | `geoarrow.multipolygon` |
| more than one family, or any collection | `geoarrow.geometry`     |

Narrowing matters in practice: `geoarrow.geometry` is a dense union that
geoarrow's R bindings cannot yet convert, so a genuinely mixed
collection reads back as its raw storage rather than as geometries.

Property columns appear in the order the keys are first seen. A key
missing from a given feature is null for that row. Types are widened
across features:

|                                       |                  |
|---------------------------------------|------------------|
| values seen for a key                 | column type      |
| booleans                              | `Boolean`        |
| integers                              | `Int64`          |
| integers and reals                    | `Float64`        |
| strings, or any mix that cannot unify | `Utf8`           |
| only `null`                           | `Utf8`, all null |

Nested arrays and objects have no arrow analogue and are kept as their
raw JSON text.

Per RFC 7946 the coordinate reference system is always `OGC:CRS84`, so
that is set on the geometry column without reading anything from the
file.

## See also

Other io:
[`read_flatgeobuf()`](https://josiahparry.github.io/geoarrowrs/reference/read_flatgeobuf.md),
[`read_shapefile()`](https://josiahparry.github.io/geoarrowrs/reference/read_shapefile.md)

## Examples

``` r
path <- tempfile(fileext = ".geojson")
fp <- system.file("shape/nc.shp", package = "sf")
sf::st_write(
  sf::st_read(fp, quiet = TRUE),
  path,
  quiet = TRUE
)

# read into a nanoarrow array stream
res <- read_geojson(path)
res
#> <nanoarrow_array_stream struct<AREA: double, PERIMETER: double, CNTY_: double, CNTY_ID: double, NAME: string, FIPS: string, FIPSNO: double, CRESS_ID: int64, BIR74: double, SID74: double, NWBIR74: double, BIR79: double, SID79: double, NWBIR79: double, geometry: geoarrow.multipolygon{list<polygons: list<rings: list<vertices: struct<x: double, y: double>>>>}>>
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
