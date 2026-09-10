# Read spatial files

Three readers. One shape of output.

**Why it matters:** every reader hands back a record batch stream, so
the data is Arrow from the first byte. Nothing is converted to an R list
of geometries on the way in.

[`library`](https://rdrr.io/r/base/library.html)`(``geoarrowrs``)`` `[`library`](https://rdrr.io/r/base/library.html)`(`[`geoarrow`](https://geoarrow.org/geoarrow-r/)`)`

## Shapefiles

[`read_shapefile()`](https://josiahparry.github.io/geoarrowrs/reference/read_shapefile.md)
reads the `.shp` and its `.dbf` attributes together.

`path`` ``<-`` `[`system.file`](https://rdrr.io/r/base/system.file.html)`(``"shape/nc.shp"``, package ``=`` ``"sf"``)`` ``nc`` ``<-`` `[`as.data.frame`](https://rdrr.io/r/base/as.data.frame.html)`(`[`read_shapefile`](https://josiahparry.github.io/geoarrowrs/reference/read_shapefile.md)`(``path``)``)`` `` `[`dim`](https://rdrr.io/r/base/dim.html)`(``nc``)`` ``#> [1] 100 15`` `[`head`](https://rdrr.io/r/utils/head.html)`(``nc``[``, `[`c`](https://rdrr.io/r/base/c.html)`(``"NAME"``, ``"BIR74"``, ``"geometry"``)``]``, ``3``)`` ``#> NAME BIR74`` ``#> 1 Ashe 1091`` ``#> 2 Alleghany 487`` ``#> 3 Surry 3188`` ``#> geometry`` ``#> 1 <MULTIPOLYGON (((-81.4727554 36.2343559, -81.5408401 36.2725067, -81.56>`` ``#> 2 <MULTIPOLYGON (((-81.2398911 36.3653641, -81.2406921 36.3794174, -81.26>`` ``#> 3 <MULTIPOLYGON (((-80.4563446 36.2425575, -80.476387 36.2547264, -80.536>`

The attribute columns come first, `geometry` last.

**Z and M work.** Both are detected from the coordinate data, not the
file header. The header records a `0` range when M is absent, which is
indistinguishable from a real measure of zero, so it cannot be trusted.

## GeoJSON

`gj`` ``<-`` `[`tempfile`](https://rdrr.io/r/base/tempfile.html)`(``fileext ``=`` ``".geojson"``)`` ``sf``::`[`st_write`](https://r-spatial.github.io/sf/reference/st_write.html)`(``sf``::`[`st_read`](https://r-spatial.github.io/sf/reference/st_read.html)`(``path``, quiet ``=`` ``TRUE``)``, ``gj``, quiet ``=`` ``TRUE``)`` `` `[`as.data.frame`](https://rdrr.io/r/base/as.data.frame.html)`(`[`read_geojson`](https://josiahparry.github.io/geoarrowrs/reference/read_geojson.md)`(``gj``)``)``[``1``:``3``, `[`c`](https://rdrr.io/r/base/c.html)`(``"NAME"``, ``"geometry"``)``]`` ``#> NAME`` ``#> 1 Ashe`` ``#> 2 Alleghany`` ``#> 3 Surry`` ``#> geometry`` ``#> 1 <MULTIPOLYGON (((-81.4727554 36.2343559, -81.5408401 36.2725067, -81.56>`` ``#> 2 <MULTIPOLYGON (((-81.2398911 36.3653641, -81.2406921 36.3794174, -81.26>`` ``#> 3 <MULTIPOLYGON (((-80.4563446 36.2425575, -80.476387 36.2547264, -80.536>`

Two things worth knowing:

- **Property order is preserved.** Columns come back in document order,
  not sorted.
- **Types widen across features.** A property that is an integer in one
  feature and a double in the next becomes a double column.

## FlatGeobuf

The only reader that truly streams, and the only one with a spatial
index.

`fgb`` ``<-`` `[`tempfile`](https://rdrr.io/r/base/tempfile.html)`(``fileext ``=`` ``".fgb"``)`` ``sf``::`[`st_write`](https://r-spatial.github.io/sf/reference/st_write.html)`(``sf``::`[`st_read`](https://r-spatial.github.io/sf/reference/st_read.html)`(``path``, quiet ``=`` ``TRUE``)``, ``fgb``, quiet ``=`` ``TRUE``)`` `` `[`nrow`](https://rdrr.io/r/base/nrow.html)`(`[`as.data.frame`](https://rdrr.io/r/base/as.data.frame.html)`(`[`read_flatgeobuf`](https://josiahparry.github.io/geoarrowrs/reference/read_flatgeobuf.md)`(``fgb``)``)``)`` ``#> [1] 100`

Pass a bounding box and the reader uses the file’s packed Hilbert R-tree
to skip everything outside it. It never decodes the features you did not
ask for.

[`nrow`](https://rdrr.io/r/base/nrow.html)`(`[`as.data.frame`](https://rdrr.io/r/base/as.data.frame.html)`(`[`read_flatgeobuf`](https://josiahparry.github.io/geoarrowrs/reference/read_flatgeobuf.md)`(``fgb``, bbox ``=`` `[`c`](https://rdrr.io/r/base/c.html)`(``-``79``, ``35``, ``-``78``, ``36``)``)``)``)`` ``#> [1] 15`

## What about GeoParquet?

Use [geoarrow](https://geoarrow.org/geoarrow-r/) and
[arrow](https://github.com/apache/arrow/). It is already handled well
there, so this package does not duplicate it.

## The bottom line

Read into Arrow, stay in Arrow. The next article shows how to compute
without ever leaving it.
