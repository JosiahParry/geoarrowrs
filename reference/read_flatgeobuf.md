# Read a FlatGeobuf file into a record batch stream

Reads the geometries together with the feature properties. Unlike the
shapefile and GeoJSON readers this one streams, so the whole file is
never held in memory at once.

## Usage

``` r
read_flatgeobuf(path, bbox = NULL)
```

## Arguments

- path:

  path to a `.fgb` file.

- bbox:

  optionally a length 4 numeric vector of `c(xmin, ymin, xmax, ymax)`
  used to filter features spatially. `NULL` reads every feature.

## Value

a `nanoarrow_array_stream` of record batches, holding the property
columns followed by a `geometry` column.

## Details

Passing `bbox` uses the file's packed Hilbert R-tree index to skip
features that fall outside it, so a spatial subset does not read the
whole file. Features come back in the order the file stores them, which
for an indexed file is that R-tree order rather than the order they were
written in.

## See also

Other io:
[`read_geojson()`](https://josiahparry.github.io/geoarrowrs/reference/read_geojson.md),
[`read_shapefile()`](https://josiahparry.github.io/geoarrowrs/reference/read_shapefile.md)

## Examples

``` r
path <- tempfile(fileext = ".fgb")
fp <- system.file("shape/nc.shp", package = "sf")
sf::st_write(
  sf::st_read(fp, quiet = TRUE),
  path,
  quiet = TRUE
)

# read into a nanoarrow array stream
res <- read_flatgeobuf(path)
res
#> <nanoarrow_array_stream struct<AREA: double, PERIMETER: double, CNTY_: double, CNTY_ID: double, NAME: string_view, FIPS: string_view, FIPSNO: double, CRESS_ID: int32, BIR74: double, SID74: double, NWBIR74: double, BIR79: double, SID79: double, NWBIR79: double, geometry: geoarrow.multipolygon{list<polygons: list<rings: list<vertices: struct<x: double, y: double>>>>}>>
#>  $ get_schema:function ()  
#>  $ get_next  :function (schema = x$get_schema(), validate = TRUE)  
#>  $ release   :function ()  

# convert to a df
df <- as.data.frame(res)
head(df)
#>    AREA PERIMETER CNTY_ CNTY_ID        NAME  FIPS FIPSNO CRESS_ID BIR74 SID74
#> 1 0.212     2.024  2241    2241   Brunswick 37019  37019       10  2181     5
#> 2 0.240     2.365  2232    2232    Columbus 37047  37047       24  3350    15
#> 3 0.042     0.999  2238    2238 New Hanover 37129  37129       65  5526    12
#> 4 0.214     2.152  2185    2185      Pender 37141  37141       71  1228     4
#> 5 0.225     2.107  2162    2162      Bladen 37017  37017        9  1782     8
#> 6 0.240     2.004  2150    2150     Robeson 37155  37155       78  7889    31
#>   NWBIR74 BIR79 SID79 NWBIR79
#> 1     659  2655     6     841
#> 2    1431  4144    17    1832
#> 3    1633  6917     9    2100
#> 4     580  1602     3     763
#> 5     818  2052     5    1023
#> 6    5904  9087    26    6899
#>                                                                   geometry
#> 1 <MULTIPOLYGON (((-78.6557159 33.9486732, -78.6347198 33.9779778, -78.63>
#> 2 <MULTIPOLYGON (((-78.6557159 33.9486732, -79.074501 34.3045731, -79.040>
#> 3 <MULTIPOLYGON (((-77.9607315 34.1892433, -77.9658661 34.2422867, -77.97>
#> 4 <MULTIPOLYGON (((-78.0259247 34.3287697, -78.1302414 34.3641243, -78.15>
#> 5 <MULTIPOLYGON (((-78.2614975 34.3947868, -78.3289795 34.3644218, -78.43>
#> 6 <MULTIPOLYGON (((-78.8645096 34.4771957, -78.9194717 34.45364, -78.9507>

# only the features intersecting a box
head(as.data.frame(read_flatgeobuf(path, c(-79, 35, -78, 36))))
#>    AREA PERIMETER CNTY_ CNTY_ID       NAME  FIPS FIPSNO CRESS_ID BIR74 SID74
#> 1 0.172     1.835  2090    2090 Cumberland 37051  37051       26 20366    38
#> 2 0.241     2.214  2083    2083    Sampson 37163  37163       82  3025     4
#> 3 0.204     1.871  2100    2100     Duplin 37061  37061       31  2483     4
#> 4 0.190     2.204  1846    1846    Halifax 37083  37083       42  3608    18
#> 5 0.142     1.640  1913    1913       Nash 37127  37127       64  4021     8
#> 6 0.128     1.554  1897    1897   Franklin 37069  37069       35  1399     2
#>   NWBIR74 BIR79 SID79 NWBIR79
#> 1    7043 26370    57   10614
#> 2    1396  3447     4    1524
#> 3    1061  2777     7    1227
#> 4    2365  4463    17    2980
#> 5    1851  5189     7    2274
#> 6     736  1863     0     950
#>                                                                   geometry
#> 1 <MULTIPOLYGON (((-78.4992905 34.8551064, -78.5174408 34.8435287, -78.83>
#> 2 <MULTIPOLYGON (((-78.1137695 34.7209854, -78.113739 34.6991806, -78.156>
#> 3 <MULTIPOLYGON (((-77.6898346 34.7201958, -77.9266739 34.7110062, -77.93>
#> 4 <MULTIPOLYGON (((-77.3322067 36.0679817, -77.4053116 35.9947166, -77.42>
#> 5 <MULTIPOLYGON (((-78.1869278 35.7251129, -78.2056198 35.7253952, -78.21>
#> 6 <MULTIPOLYGON (((-78.2545471 35.8155251, -78.2668457 35.8483772, -78.30>
```
