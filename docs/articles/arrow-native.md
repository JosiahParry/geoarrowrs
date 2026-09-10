# dplyr and Arrow

Spatial functions that run inside Arrow’s engine, not in R.

**Why it matters:** Arrow only knows the functions registered with it.
Call anything else on a `Table` and it warns, then pulls your data into
R. On a `Dataset` bigger than memory, that fails.

[`library`](https://rdrr.io/r/base/library.html)`(``geoarrowrs``)`` `[`library`](https://rdrr.io/r/base/library.html)`(`[`arrow`](https://github.com/apache/arrow/)`)`` `[`library`](https://rdrr.io/r/base/library.html)`(`[`dplyr`](https://dplyr.tidyverse.org)`)`` `[`library`](https://rdrr.io/r/base/library.html)`(`[`geoarrow`](https://geoarrow.org/geoarrow-r/)`)`

## Register once

`nc`` ``<-`` `[`as.data.frame`](https://rdrr.io/r/base/as.data.frame.html)`(`[`read_shapefile`](https://josiahparry.github.io/geoarrowrs/reference/read_shapefile.md)`(`` `` `[`system.file`](https://rdrr.io/r/base/system.file.html)`(``"shape/nc.shp"``, package ``=`` ``"sf"``)`` ``)``)`` ``tbl`` ``<-`` `[`arrow_table`](https://arrow.apache.org/docs/r/reference/table.html)`(``nc``)`` `` `[`register_geoarrow_udfs`](https://josiahparry.github.io/geoarrowrs/reference/register_geoarrow_udfs.md)`(``tbl``)`` ``#> Warning: Could not register 5 functions for this geometry type.`` ``#> ``ℹ```  `length_euclidean()`, `length_haversine()`, `length_geodesic()`, ``` ``` #> `length_rhumb()`, and `length_vincenty()` ``` ``#> ``✖`` First reason: Invalid argument error: Extension type name mismatch: expected`` ``#> geoarrow.linestring, got geoarrow.multipolygon`

## Then write normal dplyr

`tbl`` ``|>`` `` `[`mutate`](https://dplyr.tidyverse.org/reference/mutate.html)`(``area ``=`` `[`unsigned_area`](https://josiahparry.github.io/geoarrowrs/reference/area.md)`(``geometry``)``)`` ``|>`` `` `[`select`](https://dplyr.tidyverse.org/reference/select.html)`(``NAME``, ``area``)`` ``|>`` `` `[`head`](https://rdrr.io/r/utils/head.html)`(``3``)`` ``|>`` `` `[`collect`](https://dplyr.tidyverse.org/reference/compute.html)`(``)`` ``#> ``# A tibble: 3 × 2`` ``#> NAME area`` ``#> ``<chr>`` ``<dbl>`` ``#> ``1`` Ashe 0.114 `` ``#> ``2`` Alleghany 0.061``4`` ``#> ``3`` Surry 0.143`

**Filter on a predicate.** The comparison happens in the engine.

`tbl`` ``|>`` `` `[`filter`](https://dplyr.tidyverse.org/reference/filter.html)`(`[`unsigned_area`](https://josiahparry.github.io/geoarrowrs/reference/area.md)`(``geometry``)`` ``>`` ``0.1``)`` ``|>`` `` `[`summarise`](https://dplyr.tidyverse.org/reference/summarise.html)`(``n ``=`` `[`n`](https://dplyr.tidyverse.org/reference/context.html)`(``)``)`` ``|>`` `` `[`collect`](https://dplyr.tidyverse.org/reference/compute.html)`(``)`` ``#> ``# A tibble: 1 × 1`` ``#> n`` ``#> ``<int>`` ``#> ``1`` 66`

**Geometry in, geometry out.** The result keeps its GeoArrow type.

`tbl`` ``|>`` `` `[`mutate`](https://dplyr.tidyverse.org/reference/mutate.html)`(``centre ``=`` `[`centroid`](https://josiahparry.github.io/geoarrowrs/reference/centroid.md)`(``geometry``)``)`` ``|>`` `` `[`pull`](https://dplyr.tidyverse.org/reference/pull.html)`(``centre``)`` ``|>`` `` `[`class`](https://rdrr.io/r/base/class.html)`(``)`` ``` #> Warning: Default behavior of `pull()` on Arrow data is changing. Current behavior of returning an R vector is deprecated, and in a future release, it will return an Arrow `ChunkedArray`. To control this: ``` ``#> ``ℹ```  Specify `as_vector = TRUE` (the current default) or `FALSE` (what it will change to) in `pull()` ``` ``#> ``ℹ```  Or, set `options(arrow.pull_as_vector)` globally ``` ``#> ``This warning is displayed once every 8 hours.`` ``#> [1] "geoarrow_vctr" "nanoarrow_vctr"`

**Datasets stream.** Nothing here holds the column in memory.

`dir`` ``<-`` `[`file.path`](https://rdrr.io/r/base/file.path.html)`(`[`tempdir`](https://rdrr.io/r/base/tempfile.html)`(``)``, ``"nc-dataset"``)`` `[`unlink`](https://rdrr.io/r/base/unlink.html)`(``dir``, recursive ``=`` ``TRUE``)`` `[`write_dataset`](https://arrow.apache.org/docs/r/reference/write_dataset.html)`(``tbl``, ``dir``, format ``=`` ``"feather"``)`` `` `[`open_dataset`](https://arrow.apache.org/docs/r/reference/open_dataset.html)`(``dir``, format ``=`` ``"feather"``)`` ``|>`` `` `[`filter`](https://dplyr.tidyverse.org/reference/filter.html)`(`[`unsigned_area`](https://josiahparry.github.io/geoarrowrs/reference/area.md)`(``geometry``)`` ``>`` ``0.1``)`` ``|>`` `` `[`summarise`](https://dplyr.tidyverse.org/reference/summarise.html)`(``n ``=`` `[`n`](https://dplyr.tidyverse.org/reference/context.html)`(``)``)`` ``|>`` `` `[`collect`](https://dplyr.tidyverse.org/reference/compute.html)`(``)`` ``#> ``# A tibble: 1 × 1`` ``#> n`` ``#> ``<int>`` ``#> ``1`` 66`

## What gets skipped, and why

A warning names anything that could not register. Three reasons:

- **Wrong type.**
  [`length_euclidean()`](https://josiahparry.github.io/geoarrowrs/reference/length.md)
  needs linestrings. Against multipolygons there is nothing to measure.
- **Non-geometry arguments.**
  [`buffer()`](https://josiahparry.github.io/geoarrowrs/reference/buffer.md)
  takes a distance. These kernels take geometry columns only.
- **Aggregates.**
  [`unary_union()`](https://josiahparry.github.io/geoarrowrs/reference/unary_union.md)
  collapses an array to one row, which is not a scalar kernel.

## Two rules

**Register per geometry type.** Arrow dispatches on the exact input
type, so reading a different type means calling
[`register_geoarrow_udfs()`](https://josiahparry.github.io/geoarrowrs/reference/register_geoarrow_udfs.md)
again with that data.

**Use `prefix` to avoid collisions** with an existing Arrow binding.

[`head`](https://rdrr.io/r/utils/head.html)`(`[`register_geoarrow_udfs`](https://josiahparry.github.io/geoarrowrs/reference/register_geoarrow_udfs.md)`(``tbl``, prefix ``=`` ``"geo_"``)``, ``3``)`` ``#> Warning: Could not register 5 functions for this geometry type.`` ``#> ``ℹ```  `length_euclidean()`, `length_haversine()`, `length_geodesic()`, ``` ``` #> `length_rhumb()`, and `length_vincenty()` ``` ``#> ``✖`` First reason: Invalid argument error: Extension type name mismatch: expected`` ``#> geoarrow.linestring, got geoarrow.multipolygon`` ``#> [1] "geo_signed_area" "geo_unsigned_area" "geo_signed_area_cd"`

## The bottom line

One call, then ordinary dplyr. The geometry stays in Arrow.
