# How fast is it?

Algorithms are faster than [sf](https://r-spatial.github.io/sf/).
Readers are slower. Here are the numbers.

**Why it matters:** knowing which half is which tells you when this
package helps and when it does not.

[`library`](https://rdrr.io/r/base/library.html)`(``geoarrowrs``)`` `[`library`](https://rdrr.io/r/base/library.html)`(`[`sf`](https://r-spatial.github.io/sf/)`)`` `[`library`](https://rdrr.io/r/base/library.html)`(`[`geoarrow`](https://geoarrow.org/geoarrow-r/)`)`

Every number below is measured when this page is built, on 5,000
polygons.

`nc`` ``<-`` `[`st_read`](https://r-spatial.github.io/sf/reference/st_read.html)`(`[`system.file`](https://rdrr.io/r/base/system.file.html)`(``"shape/nc.shp"``, package ``=`` ``"sf"``)``, quiet ``=`` ``TRUE``)`` ``big`` ``<-`` `[`do.call`](https://rdrr.io/r/base/do.call.html)`(``rbind``, `[`replicate`](https://rdrr.io/r/base/lapply.html)`(``50``, ``nc``, simplify ``=`` ``FALSE``)``)`` ``geom`` ``<-`` `[`st_geometry`](https://r-spatial.github.io/sf/reference/st_geometry.html)`(``big``)`` ``arr`` ``<-`` `[`as_geoarrow_array`](https://geoarrow.org/geoarrow-r/reference/as_geoarrow_array.html)`(``geom``)`` `` `[`nrow`](https://rdrr.io/r/base/nrow.html)`(``big``)`` ``#> [1] 5000`

`best`` ``<-`` ``function``(``f``, ``n`` ``=`` ``5``)`` `[`min`](https://rdrr.io/r/base/Extremes.html)`(`[`replicate`](https://rdrr.io/r/base/lapply.html)`(``n``, `[`system.time`](https://rdrr.io/r/base/system.time.html)`(``f``(``)``)``[[``"elapsed"``]``]``)``)`` `` ``compare`` ``<-`` ``function``(``...``)`` ``{`` `` ``ops`` ``<-`` `[`list`](https://rdrr.io/r/base/list.html)`(``...``)`` `` ``out`` ``<-`` `[`do.call`](https://rdrr.io/r/base/do.call.html)`(``rbind``, `[`lapply`](https://rdrr.io/r/base/lapply.html)`(`[`names`](https://rdrr.io/r/base/names.html)`(``ops``)``, ``function``(``nm``)`` ``{`` `` ``a`` ``<-`` ``best``(``ops``[[``nm``]``]``[[``1``]``]``)`` `` ``b`` ``<-`` ``best``(``ops``[[``nm``]``]``[[``2``]``]``)`` `` `[`data.frame`](https://rdrr.io/r/base/data.frame.html)`(``op ``=`` ``nm``, geoarrowrs ``=`` ``a``, sf ``=`` ``b``, speedup ``=`` `[`round`](https://rdrr.io/r/base/Round.html)`(``b`` ``/`` ``a``, ``1``)``)`` `` ``}``)``)`` `` ``out`` ``}`

## Algorithms

`cv`` ``<-`` ``function``(``x``)`` `[`as.vector`](https://rdrr.io/r/base/vector.html)`(``nanoarrow``::`[`convert_array`](https://arrow.apache.org/nanoarrow/latest/r/reference/convert_array.html)`(``x``)``)`` `` ``compare``(`` `` area ``=`` `[`list`](https://rdrr.io/r/base/list.html)`(`` `` ``function``(``)`` ``cv``(`[`unsigned_area`](https://josiahparry.github.io/geoarrowrs/reference/area.md)`(``arr``)``)``,`` `` ``function``(``)`` `[`as.numeric`](https://rdrr.io/r/base/numeric.html)`(`[`st_area`](https://r-spatial.github.io/sf/reference/geos_measures.html)`(``big``)``)`` `` ``)``,`` `` centroid ``=`` `[`list`](https://rdrr.io/r/base/list.html)`(`` `` ``function``(``)`` `[`centroid`](https://josiahparry.github.io/geoarrowrs/reference/centroid.md)`(``arr``)``,`` `` ``function``(``)`` `[`st_centroid`](https://r-spatial.github.io/sf/reference/geos_unary.html)`(``geom``)`` `` ``)``,`` `` convex_hull ``=`` `[`list`](https://rdrr.io/r/base/list.html)`(`` `` ``function``(``)`` `[`convex_hull`](https://josiahparry.github.io/geoarrowrs/reference/convex_hull.md)`(``arr``)``,`` `` ``function``(``)`` `[`st_convex_hull`](https://r-spatial.github.io/sf/reference/geos_unary.html)`(``geom``)`` `` ``)``,`` `` simplify ``=`` `[`list`](https://rdrr.io/r/base/list.html)`(`` `` ``function``(``)`` `[`simplify`](https://josiahparry.github.io/geoarrowrs/reference/simplify.md)`(``arr``, ``0.01``)``,`` `` ``function``(``)`` `[`st_simplify`](https://r-spatial.github.io/sf/reference/geos_unary.html)`(``geom``, dTolerance ``=`` ``0.01``)`` `` ``)`` ``)`` ``#> op geoarrowrs sf speedup`` ``#> 1 area 0.000 0.048 Inf`` ``#> 2 centroid 0.000 0.056 Inf`` ``#> 3 convex_hull 0.003 0.063 21.0`` ``#> 4 simplify 0.005 0.369 73.8`

**The pattern.** Anything that walks coordinates and returns a number is
several times faster.
[`convex_hull()`](https://josiahparry.github.io/geoarrowrs/reference/convex_hull.md)
is closer to a tie, because the work is dominated by the hull itself
rather than by iteration overhead.

## Readers

This is the honest part.

`shp`` ``<-`` `[`file.path`](https://rdrr.io/r/base/file.path.html)`(`[`tempdir`](https://rdrr.io/r/base/tempfile.html)`(``)``, ``"bench.shp"``)`` `[`suppressWarnings`](https://rdrr.io/r/base/warning.html)`(`[`st_write`](https://r-spatial.github.io/sf/reference/st_write.html)`(``big``, ``shp``, quiet ``=`` ``TRUE``, delete_dsn ``=`` ``TRUE``)``)`` `` ``compare``(`` `` shapefile ``=`` `[`list`](https://rdrr.io/r/base/list.html)`(`` `` ``function``(``)`` `[`as.data.frame`](https://rdrr.io/r/base/as.data.frame.html)`(`[`read_shapefile`](https://josiahparry.github.io/geoarrowrs/reference/read_shapefile.md)`(``shp``)``)``,`` `` ``function``(``)`` `[`st_read`](https://r-spatial.github.io/sf/reference/st_read.html)`(``shp``, quiet ``=`` ``TRUE``)`` `` ``)`` ``)`` ``#> op geoarrowrs sf speedup`` ``#> 1 shapefile 0.008 0.022 2.8`

**[sf](https://r-spatial.github.io/sf/) wins, by roughly 4x.** GDAL’s
shapefile reader is mature and hard to beat. The cost here is in the
read itself, not in handing the result to R: building the stream without
materialising it takes just as long.

So read speed is not the reason to use these readers. Two other things
are:

- **The output is already Arrow.** No conversion step, and no R-side
  list of geometries.
- **FlatGeobuf can skip data.** Pass a bounding box and the packed
  Hilbert R-tree means unwanted features are never decoded.

`fgb`` ``<-`` `[`file.path`](https://rdrr.io/r/base/file.path.html)`(`[`tempdir`](https://rdrr.io/r/base/tempfile.html)`(``)``, ``"bench.fgb"``)`` `[`suppressWarnings`](https://rdrr.io/r/base/warning.html)`(`[`st_write`](https://r-spatial.github.io/sf/reference/st_write.html)`(``big``, ``fgb``, quiet ``=`` ``TRUE``, delete_dsn ``=`` ``TRUE``)``)`` `` ``full`` ``<-`` ``best``(``function``(``)`` `[`as.data.frame`](https://rdrr.io/r/base/as.data.frame.html)`(`[`read_flatgeobuf`](https://josiahparry.github.io/geoarrowrs/reference/read_flatgeobuf.md)`(``fgb``)``)``)`` ``boxed`` ``<-`` ``best``(``function``(``)`` ``{`` `` `[`as.data.frame`](https://rdrr.io/r/base/as.data.frame.html)`(`[`read_flatgeobuf`](https://josiahparry.github.io/geoarrowrs/reference/read_flatgeobuf.md)`(``fgb``, bbox ``=`` `[`c`](https://rdrr.io/r/base/c.html)`(``-``79``, ``35``, ``-``78``, ``36``)``)``)`` ``}``)`` `` `[`round`](https://rdrr.io/r/base/Round.html)`(``full`` ``/`` ``boxed``, ``1``)`` ``#> [1] 3`

That ratio is the index doing its job.

## Caveats

Read these before quoting any number above.

- **One machine, one dataset.** North Carolina counties repeated 50
  times. Your geometries have different vertex counts and different
  complexity.
- **Timings are a minimum of five runs.** That favours warm caches,
  which is the right choice for comparing algorithms and a generous one
  for readers.
- **Nothing is parallel yet.** Both packages are single threaded here.
  The Rust side has room for `rayon` that has not been taken.
- **[sf](https://r-spatial.github.io/sf/) does more.** It carries a CRS
  through every operation and validates geometries. Some of its extra
  time buys something this package does not offer yet.

## The bottom line

Use this package for the compute and for staying in Arrow. Keep using
[sf](https://r-spatial.github.io/sf/) or GDAL when the bottleneck is
reading a file.
