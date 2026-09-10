# Compute inside Arrow with dplyr

Spatial functions that run in Arrow’s query engine, not in R.

**Why it matters:** a `Dataset` can be larger than memory. If every
spatial call drags the geometry column into R, that advantage is gone.

[`library`](https://rdrr.io/r/base/library.html)`(``geoarrowrs``)`` `[`library`](https://rdrr.io/r/base/library.html)`(`[`arrow`](https://github.com/apache/arrow/)`)`` `[`library`](https://rdrr.io/r/base/library.html)`(`[`dplyr`](https://dplyr.tidyverse.org)`)`` `[`library`](https://rdrr.io/r/base/library.html)`(`[`geoarrow`](https://geoarrow.org/geoarrow-r/)`)`

## The problem

Arrow only knows the functions registered with it. Call anything else
and it gives up and pulls your data into R.

`nc`` ``<-`` `[`as.data.frame`](https://rdrr.io/r/base/as.data.frame.html)`(`[`read_shapefile`](https://josiahparry.github.io/geoarrowrs/reference/read_shapefile.md)`(`` `` `[`system.file`](https://rdrr.io/r/base/system.file.html)`(``"shape/nc.shp"``, package ``=`` ``"sf"``)`` ``)``)`` ``tbl`` ``<-`` `[`arrow_table`](https://arrow.apache.org/docs/r/reference/table.html)`(``nc``)`

Without registration you get a warning and a fallback:

    Warning: Expression not supported in Arrow
    → Pulling data into R

On a `Table` that is merely slow. On a `Dataset` that does not fit in
memory, it fails outright.

## The fix

One call.

`registered`` ``<-`` `[`register_geoarrow_udfs`](https://josiahparry.github.io/geoarrowrs/reference/register_geoarrow_udfs.md)`(``tbl``)`` ``#> Warning: Could not register 5 functions for this geometry type.`` ``#> ``ℹ```  `length_euclidean()`, `length_haversine()`, `length_geodesic()`, ``` ``` #> `length_rhumb()`, and `length_vincenty()` ``` ``#> ``✖`` First reason: Invalid argument error: Extension type name mismatch: expected`` ``#> geoarrow.linestring, got geoarrow.multipolygon`` `[`length`](https://rdrr.io/r/base/length.html)`(``registered``)`` ``#> [1] 46`

Now the same code runs inside the engine.

`tbl`` ``|>`` `` `[`mutate`](https://dplyr.tidyverse.org/reference/mutate.html)`(``area ``=`` `[`unsigned_area`](https://josiahparry.github.io/geoarrowrs/reference/area.md)`(``geometry``)``)`` ``|>`` `` `[`select`](https://dplyr.tidyverse.org/reference/select.html)`(``NAME``, ``area``)`` ``|>`` `` `[`head`](https://rdrr.io/r/utils/head.html)`(``3``)`` ``|>`` `` `[`collect`](https://dplyr.tidyverse.org/reference/compute.html)`(``)`` ``#> ``# A tibble: 3 × 2`` ``#> NAME area`` ``#> ``<chr>`` ``<dbl>`` ``#> ``1`` Ashe 0.114 `` ``#> ``2`` Alleghany 0.061``4`` ``#> ``3`` Surry 0.143`

## It composes

**Filter on a spatial predicate.** The comparison happens in the engine.

`tbl`` ``|>`` `` `[`filter`](https://dplyr.tidyverse.org/reference/filter.html)`(`[`unsigned_area`](https://josiahparry.github.io/geoarrowrs/reference/area.md)`(``geometry``)`` ``>`` ``0.1``)`` ``|>`` `` `[`summarise`](https://dplyr.tidyverse.org/reference/summarise.html)`(``n ``=`` `[`n`](https://dplyr.tidyverse.org/reference/context.html)`(``)``)`` ``|>`` `` `[`collect`](https://dplyr.tidyverse.org/reference/compute.html)`(``)`` ``#> ``# A tibble: 1 × 1`` ``#> n`` ``#> ``<int>`` ``#> ``1`` 66`

**Geometry in, geometry out.** The result keeps its GeoArrow type.

`res`` ``<-`` ``tbl`` ``|>`` `` `[`mutate`](https://dplyr.tidyverse.org/reference/mutate.html)`(``centre ``=`` `[`centroid`](https://josiahparry.github.io/geoarrowrs/reference/centroid.md)`(``geometry``)``)`` ``|>`` `` `[`select`](https://dplyr.tidyverse.org/reference/select.html)`(``NAME``, ``centre``)`` ``|>`` `` `[`head`](https://rdrr.io/r/utils/head.html)`(``3``)`` ``|>`` `` `[`collect`](https://dplyr.tidyverse.org/reference/compute.html)`(``)`` `` `[`class`](https://rdrr.io/r/base/class.html)`(``res``$``centre``)`` ``#> [1] "geoarrow_vctr" "nanoarrow_vctr"`

**Predicates take two geometry columns.**

`tbl`` ``|>`` `` `[`mutate`](https://dplyr.tidyverse.org/reference/mutate.html)`(``self ``=`` `[`intersects`](https://josiahparry.github.io/geoarrowrs/reference/topology.md)`(``geometry``, ``geometry``)``)`` ``|>`` `` `[`summarise`](https://dplyr.tidyverse.org/reference/summarise.html)`(``all_true ``=`` `[`all`](https://rdrr.io/r/base/all.html)`(``self``)``)`` ``|>`` `` `[`collect`](https://dplyr.tidyverse.org/reference/compute.html)`(``)`` ``#> ``# A tibble: 1 × 1`` ``#> all_true`` ``#> ``<lgl>`` `` ``#> ``1`` TRUE`

## Datasets stream

This is the case that matters. Nothing here holds the whole column in
memory.

`dir`` ``<-`` `[`file.path`](https://rdrr.io/r/base/file.path.html)`(`[`tempdir`](https://rdrr.io/r/base/tempfile.html)`(``)``, ``"nc-dataset"``)`` `[`unlink`](https://rdrr.io/r/base/unlink.html)`(``dir``, recursive ``=`` ``TRUE``)`` `[`write_dataset`](https://arrow.apache.org/docs/r/reference/write_dataset.html)`(``tbl``, ``dir``, format ``=`` ``"feather"``)`` `` `[`open_dataset`](https://arrow.apache.org/docs/r/reference/open_dataset.html)`(``dir``, format ``=`` ``"feather"``)`` ``|>`` `` `[`filter`](https://dplyr.tidyverse.org/reference/filter.html)`(`[`unsigned_area`](https://josiahparry.github.io/geoarrowrs/reference/area.md)`(``geometry``)`` ``>`` ``0.1``)`` ``|>`` `` `[`summarise`](https://dplyr.tidyverse.org/reference/summarise.html)`(``n ``=`` `[`n`](https://dplyr.tidyverse.org/reference/context.html)`(``)``)`` ``|>`` `` `[`collect`](https://dplyr.tidyverse.org/reference/compute.html)`(``)`` ``#> ``# A tibble: 1 × 1`` ``#> n`` ``#> ``<int>`` ``#> ``1`` 66`

## What gets registered

**46 of 51** functions register against a multipolygon column. The rest
are skipped on purpose, and the warning tells you which:

- **Wrong geometry type.**
  [`length_euclidean()`](https://josiahparry.github.io/geoarrowrs/reference/length.md)
  needs linestrings. Against multipolygons there is nothing to measure,
  so it is not registered.
- **Non-geometry arguments.**
  [`buffer()`](https://josiahparry.github.io/geoarrowrs/reference/buffer.md)
  and
  [`simplify()`](https://josiahparry.github.io/geoarrowrs/reference/simplify.md)
  take a distance. Arrow kernels here only take geometry columns.
- **Aggregates.**
  [`unary_union()`](https://josiahparry.github.io/geoarrowrs/reference/unary_union.md)
  collapses an array to one row. That is not a scalar kernel.

Arrow dispatches on the exact input type, so a kernel is registered for
the precise GeoArrow type in your data. Reading a different geometry
type means calling
[`register_geoarrow_udfs()`](https://josiahparry.github.io/geoarrowrs/reference/register_geoarrow_udfs.md)
again with that data.

Use `prefix` if a name would collide with an existing Arrow binding:

[`head`](https://rdrr.io/r/utils/head.html)`(`[`register_geoarrow_udfs`](https://josiahparry.github.io/geoarrowrs/reference/register_geoarrow_udfs.md)`(``tbl``, prefix ``=`` ``"geo_"``)``, ``3``)`` ``#> Warning: Could not register 5 functions for this geometry type.`` ``#> ``ℹ```  `length_euclidean()`, `length_haversine()`, `length_geodesic()`, ``` ``` #> `length_rhumb()`, and `length_vincenty()` ``` ``#> ``✖`` First reason: Invalid argument error: Extension type name mismatch: expected`` ``#> geoarrow.linestring, got geoarrow.multipolygon`` ``#> [1] "geo_signed_area" "geo_unsigned_area" "geo_signed_area_cd"`

## The bottom line

Register once, then write ordinary dplyr. The geometry stays in Arrow
and the work happens in the engine.
