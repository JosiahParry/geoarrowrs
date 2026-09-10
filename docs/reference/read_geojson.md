# Read a GeoJSON FeatureCollection into a record batch stream

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
sf::st_write(sf::st_read(system.file("shape/nc.shp", package = "sf"), quiet = TRUE),
             path, quiet = TRUE)

df <- as.data.frame(read_geojson(path))
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
