# How fast is it?

Faster than [sf](https://r-spatial.github.io/sf/) at everything measured
here. Often by a lot.

**Why it matters:** the geometry never becomes an R object. There is no
list of `sfg` classes to allocate, walk, and garbage collect, so the
work happens at Rust speed on contiguous Arrow buffers.

[`library`](https://rdrr.io/r/base/library.html)`(``geoarrowrs``)`` `[`library`](https://rdrr.io/r/base/library.html)`(`[`sf`](https://r-spatial.github.io/sf/)`)`` `[`library`](https://rdrr.io/r/base/library.html)`(`[`geoarrow`](https://geoarrow.org/geoarrow-r/)`)`

Every number below is measured when this page is built.

`nc`` ``<-`` `[`st_read`](https://r-spatial.github.io/sf/reference/st_read.html)`(`[`system.file`](https://rdrr.io/r/base/system.file.html)`(``"shape/nc.shp"``, package ``=`` ``"sf"``)``, quiet ``=`` ``TRUE``)`` ``big`` ``<-`` `[`do.call`](https://rdrr.io/r/base/do.call.html)`(``rbind``, `[`replicate`](https://rdrr.io/r/base/lapply.html)`(``200``, ``nc``, simplify ``=`` ``FALSE``)``)`` ``geom`` ``<-`` `[`st_geometry`](https://r-spatial.github.io/sf/reference/st_geometry.html)`(``big``)`` ``arr`` ``<-`` `[`as_geoarrow_array`](https://geoarrow.org/geoarrow-r/reference/as_geoarrow_array.html)`(``geom``)`` `` `[`format`](https://rdrr.io/r/base/format.html)`(`[`nrow`](https://rdrr.io/r/base/nrow.html)`(``big``)``, big.mark ``=`` ``","``)`` ``#> [1] "20,000"`

## Measuring honestly

Several of these operations finish in well under a millisecond, which
[`system.time()`](https://rdrr.io/r/base/system.time.html) cannot
resolve. Timing a single call would report `0.000` and an infinite
speedup.

So each expression is run enough times to take at least a quarter
second, and the total is divided by the count.

`per_call`` ``<-`` ``function``(``f``, ``min_time`` ``=`` ``0.25``)`` ``{`` `` ``f``(``)`` `` ``reps`` ``<-`` ``1L`` `` ``repeat`` ``{`` `` ``elapsed`` ``<-`` `[`system.time`](https://rdrr.io/r/base/system.time.html)`(``for`` ``(``i`` ``in`` `[`seq_len`](https://rdrr.io/r/base/seq.html)`(``reps``)``)`` ``f``(``)``)``[[``"elapsed"``]``]`` `` ``if`` ``(``elapsed`` ``>=`` ``min_time`` ``||`` ``reps`` ``>=`` ``1e6L``)`` ``break`` `` ``reps`` ``<-`` ``reps`` ``*`` ``4L`` `` ``}`` `` ``elapsed`` ``/`` ``reps`` ``}`` `` ``compare`` ``<-`` ``function``(``...``)`` ``{`` `` ``ops`` ``<-`` `[`list`](https://rdrr.io/r/base/list.html)`(``...``)`` `` `[`do.call`](https://rdrr.io/r/base/do.call.html)`(``rbind``, `[`lapply`](https://rdrr.io/r/base/lapply.html)`(`[`names`](https://rdrr.io/r/base/names.html)`(``ops``)``, ``function``(``nm``)`` ``{`` `` ``a`` ``<-`` ``per_call``(``ops``[[``nm``]``]``[[``1``]``]``)`` `` ``b`` ``<-`` ``per_call``(``ops``[[``nm``]``]``[[``2``]``]``)`` `` `[`data.frame`](https://rdrr.io/r/base/data.frame.html)`(`` `` op ``=`` ``nm``,`` `` geoarrowrs ``=`` `[`signif`](https://rdrr.io/r/base/Round.html)`(``a``, ``3``)``,`` `` sf ``=`` `[`signif`](https://rdrr.io/r/base/Round.html)`(``b``, ``3``)``,`` `` speedup ``=`` `[`round`](https://rdrr.io/r/base/Round.html)`(``b`` ``/`` ``a``, ``1``)`` `` ``)`` `` ``}``)``)`` ``}`

## Algorithms

`cv`` ``<-`` ``function``(``x``)`` `[`as.vector`](https://rdrr.io/r/base/vector.html)`(``nanoarrow``::`[`convert_array`](https://arrow.apache.org/nanoarrow/latest/r/reference/convert_array.html)`(``x``)``)`` `` ``compare``(`` `` area ``=`` `[`list`](https://rdrr.io/r/base/list.html)`(`` `` ``function``(``)`` ``cv``(`[`unsigned_area`](https://josiahparry.github.io/geoarrowrs/reference/area.md)`(``arr``)``)``,`` `` ``function``(``)`` `[`as.numeric`](https://rdrr.io/r/base/numeric.html)`(`[`st_area`](https://r-spatial.github.io/sf/reference/geos_measures.html)`(``big``)``)`` `` ``)``,`` `` centroid ``=`` `[`list`](https://rdrr.io/r/base/list.html)`(`` `` ``function``(``)`` `[`centroid`](https://josiahparry.github.io/geoarrowrs/reference/centroid.md)`(``arr``)``,`` `` ``function``(``)`` `[`st_centroid`](https://r-spatial.github.io/sf/reference/geos_unary.html)`(``geom``)`` `` ``)``,`` `` convex_hull ``=`` `[`list`](https://rdrr.io/r/base/list.html)`(`` `` ``function``(``)`` `[`convex_hull`](https://josiahparry.github.io/geoarrowrs/reference/convex_hull.md)`(``arr``)``,`` `` ``function``(``)`` `[`st_convex_hull`](https://r-spatial.github.io/sf/reference/geos_unary.html)`(``geom``)`` `` ``)``,`` `` simplify ``=`` `[`list`](https://rdrr.io/r/base/list.html)`(`` `` ``function``(``)`` `[`simplify`](https://josiahparry.github.io/geoarrowrs/reference/simplify.md)`(``arr``, ``0.01``)``,`` `` ``function``(``)`` `[`st_simplify`](https://r-spatial.github.io/sf/reference/geos_unary.html)`(``geom``, dTolerance ``=`` ``0.01``)`` `` ``)``,`` `` bounding_rect ``=`` `[`list`](https://rdrr.io/r/base/list.html)`(`` `` ``function``(``)`` `[`bounding_rect`](https://josiahparry.github.io/geoarrowrs/reference/bounding_rect.md)`(``arr``)``,`` `` ``function``(``)`` `[`lapply`](https://rdrr.io/r/base/lapply.html)`(``geom``, ``st_bbox``)`` `` ``)`` ``)`` ``#> op geoarrowrs sf speedup`` ``#> 1 area 0.00157 0.187 118.8`` ``#> 2 centroid 0.00209 0.247 118.2`` ``#> 3 convex_hull 0.01140 0.265 23.3`` ``#> 4 simplify 0.02120 1.470 69.3`` ``#> 5 bounding_rect 0.00204 0.104 51.2`

**The pattern.** The gap widens with how much per-geometry bookkeeping R
has to do. Operations returning a plain number are the most lopsided,
because [sf](https://r-spatial.github.io/sf/) builds and returns an R
vector while allocating nothing on our side beyond one Arrow buffer.

## Readers

`shp`` ``<-`` `[`file.path`](https://rdrr.io/r/base/file.path.html)`(`[`tempdir`](https://rdrr.io/r/base/tempfile.html)`(``)``, ``"bench.shp"``)`` ``fgb`` ``<-`` `[`file.path`](https://rdrr.io/r/base/file.path.html)`(`[`tempdir`](https://rdrr.io/r/base/tempfile.html)`(``)``, ``"bench.fgb"``)`` `[`suppressWarnings`](https://rdrr.io/r/base/warning.html)`(``{`` `` `[`st_write`](https://r-spatial.github.io/sf/reference/st_write.html)`(``big``, ``shp``, quiet ``=`` ``TRUE``, delete_dsn ``=`` ``TRUE``)`` `` `[`st_write`](https://r-spatial.github.io/sf/reference/st_write.html)`(``big``, ``fgb``, quiet ``=`` ``TRUE``, delete_dsn ``=`` ``TRUE``)`` ``}``)`` `` ``compare``(`` `` shapefile ``=`` `[`list`](https://rdrr.io/r/base/list.html)`(`` `` ``function``(``)`` `[`as.data.frame`](https://rdrr.io/r/base/as.data.frame.html)`(`[`read_shapefile`](https://josiahparry.github.io/geoarrowrs/reference/read_shapefile.md)`(``shp``)``)``,`` `` ``function``(``)`` `[`st_read`](https://r-spatial.github.io/sf/reference/st_read.html)`(``shp``, quiet ``=`` ``TRUE``)`` `` ``)``,`` `` flatgeobuf ``=`` `[`list`](https://rdrr.io/r/base/list.html)`(`` `` ``function``(``)`` `[`as.data.frame`](https://rdrr.io/r/base/as.data.frame.html)`(`[`read_flatgeobuf`](https://josiahparry.github.io/geoarrowrs/reference/read_flatgeobuf.md)`(``fgb``)``)``,`` `` ``function``(``)`` `[`st_read`](https://r-spatial.github.io/sf/reference/st_read.html)`(``fgb``, quiet ``=`` ``TRUE``)`` `` ``)`` ``)`` ``#> op geoarrowrs sf speedup`` ``#> 1 shapefile 0.0314 0.0870 2.8`` ``#> 2 flatgeobuf 0.0725 0.0865 1.2`

Reading is faster too, and the gap grows with the file. GDAL constructs
an R object per feature; these readers fill Arrow buffers and hand back
a stream.

**FlatGeobuf can also skip data outright.** Pass a bounding box and the
packed Hilbert R-tree means features outside it are never decoded.

`full`` ``<-`` ``per_call``(``function``(``)`` `[`as.data.frame`](https://rdrr.io/r/base/as.data.frame.html)`(`[`read_flatgeobuf`](https://josiahparry.github.io/geoarrowrs/reference/read_flatgeobuf.md)`(``fgb``)``)``)`` ``boxed`` ``<-`` ``per_call``(``function``(``)`` ``{`` `` `[`as.data.frame`](https://rdrr.io/r/base/as.data.frame.html)`(`[`read_flatgeobuf`](https://josiahparry.github.io/geoarrowrs/reference/read_flatgeobuf.md)`(``fgb``, bbox ``=`` `[`c`](https://rdrr.io/r/base/c.html)`(``-``79``, ``35``, ``-``78``, ``36``)``)``)`` ``}``)`` `` `[`round`](https://rdrr.io/r/base/Round.html)`(``full`` ``/`` ``boxed``, ``1``)`` ``#> [1] 12`

That ratio is the index doing its job, and it scales with how selective
the box is.

## Benchmark against a release build

**This matters more than anything else on this page.**

[`devtools::load_all()`](https://devtools.r-lib.org/reference/load_all.html)
compiles the Rust in debug. Debug Rust is not slow by a little, it is
slow by an order of magnitude, and benchmarking it will tell you this
package loses to [sf](https://r-spatial.github.io/sf/) at everything.

Measure an installed build:

`devtools``::`[`install`](https://devtools.r-lib.org/reference/install.html)`(``)`` ``# restart R, then benchmark`

An early draft of this article was written against a `load_all()`
session and reported [sf](https://r-spatial.github.io/sf/) reading files
four times faster. Rebuilt against an installed package, the same
comparison reversed.

## Caveats

- **One machine, one dataset.** North Carolina counties repeated 200
  times. Your geometries have different vertex counts and different
  complexity.
- **Nothing is parallel yet.** Both packages are single threaded here.
  The Rust side has room for `rayon` that has not been taken.
- **[sf](https://r-spatial.github.io/sf/) does more.** It carries a CRS
  through every operation and validates geometries. Some of its extra
  time buys something this package does not offer yet.
- **Z and M are dropped** by every algorithm here, because the bridge to
  the `geo` crate goes through a two dimensional geometry. The readers
  preserve them.

## The bottom line

Staying in Arrow is the whole trick. Nothing is converted to an R
object, so nothing pays for it.
