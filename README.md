
# geoarrowrs

`{geoarrowrs}` provides R bindings to the geoarrow-rs Rust crate. They
are in a very early stage.

## Usage

The flatgeobuf reader is approximately 4 times faster than `sf` in this
use case.

``` r
library(geoarrow)
library(geoarrowrs)

# get the file path
fp <- system.file("extdata", "osm-edinburgh-central.fgb", package = "geoarrowrs")

# read the flatgeobuf file
stream <- read_flatgeobuf_(fp)
stream
```

    <nanoarrow_array_stream struct<osm_id: string, name: string, highway: string, waterway: string, aerialway: string, barrier: string, man_made: string, railway: string, access: string, bicycle: string, service: string, z_order: int32, other_tags: string, geometry: geoarrow.multilinestring{list<linestrings: list<vertices: fixed_size_list(2)<xy: double>>>}>>
     $ get_schema:function ()  
     $ get_next  :function (schema = x$get_schema(), validate = TRUE)  
     $ release   :function ()  

``` r
# convert to a record batch reader
reader <- arrow::as_record_batch_reader(stream)
reader 
```

    RecordBatchReader
    14 columns
    osm_id: string
    name: string
    highway: string
    waterway: string
    aerialway: string
    barrier: string
    man_made: string
    railway: string
    access: string
    bicycle: string
    service: string
    z_order: int32
    other_tags: string
    geometry: geoarrow.multilinestring <CRS: GEOGCRS["WGS 84",ENSEMBLE["...

``` r
# convert to sf 
sf::st_as_sf(reader) |> 
    dplyr::glimpse()
```

    Rows: 7,509
    Columns: 14
    $ osm_id     <chr> "791082719", "791082720", "791082721", "1058397182", "26483…
    $ name       <chr> NA, NA, NA, NA, NA, NA, NA, "Priestfield Gardens", "Priestf…
    $ highway    <chr> "path", "path", "path", "path", "service", "path", "path", …
    $ waterway   <chr> NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA,…
    $ aerialway  <chr> NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA,…
    $ barrier    <chr> NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA,…
    $ man_made   <chr> NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA,…
    $ railway    <chr> NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA,…
    $ access     <chr> NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA,…
    $ bicycle    <chr> NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA,…
    $ service    <chr> NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA,…
    $ z_order    <int> 0, 0, -20, 0, 0, 0, 0, 3, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, …
    $ other_tags <chr> NA, NA, "\"layer\"=>\"-1\",\"tunnel\"=>\"yes\"", "\"surface…
    $ geometry   <MULTILINESTRING> MULTILINESTRING ((-3.1523 5..., MULTILINESTRING…

## Readers

- [x] FlatGeoBuf
- [x] GeoJson
- [x] GeoJsonLines
- [x] GeoParquet ⚠️ Not all options implemented
- [x] Shapefile
- [ ] wkb - going to defer to `geoarrow-r`
- [ ] wkt - going to defer to `geoarrow-r`

## Writers

- [ ] FlatGeoBuf
- [ ] GeoJson
- [ ] GeoJsonLines
- [x] GeoParquet ⚠️ Not all options implemented
- [ ] Shapefile
- [ ] wkb - going to defer to `geoarrow-r`
- [ ] wkt - going to defer to `geoarrow-r`

## Algorithm Implementations:

### Geo

- [ ] AffineOps
- [x] Area
- [x] BoundingRect
- [x] Center
- [x] Centroid
- [x] ChaikinSmoothing
- [x] ChamberlainDuquetteArea
- [x] ConcaveHull
- [x] Contains
- [x] ConvexHull
- [x] Densify
- [ ] EuclideanDistance
- [x] EuclideanLength
- [ ] FrechetDistance
- [ ] FrechetDistanceLineString
- [x] GeodesicArea
- [x] GeodesicLength
- [x] HasDimensions
- [x] HaversineLength
- [x] InteriorPoint
- [ ] Intersects
- [ ] LineInterpolatePoint
- [ ] LineLocatePoint
- [ ] LineLocatePointScalar
- [x] MinimumRotatedRect
- [x] RemoveRepeatedPoints
- [ ] Rotate
- [ ] Scale
- [ ] Simplify
- [ ] SimplifyVw
- [ ] SimplifyVwPreserve
- [ ] Skew
- [ ] Translate
- [x] VincentyLength
- [ ] Within

### Native

- [ ] Binary
- [ ] Cast
- [ ] Concatenate
- [ ] Downcast
- [ ] DowncastTable
- [ ] Explode
- [ ] ExplodeTable
- [ ] MapChunks
- [ ] MapCoords
- [ ] Rechunk
- [ ] Take
- [ ] TotalBounds
- [ ] TypeIds
- [ ] Unary
- [ ] UnaryPoint

### rstar

- [ ] RTree

### geo_index

- [ ] RTree
