# ISC milestone tracker

Objectives from the submitted proposal. Award date 2026-11-01.

Progress is `Not started`, `In progress`, or `Done`. Verified is `No` until
the user reviews the row, then `Yes (YYYY-MM-DD)`.

## Milestone 1: Core geometry engine ($1,500, due 2027-01-01)

| # | Objective | Progress | Verified | Evidence |
|---|---|---|---|---|
| 1.1 | Geometry repair: `st_make_valid()` | Done | No | `ga_make_valid()` for polygons, multipolygons, and boxes, plus sf and sdf `st_make_valid()` methods; test-make-valid.R; 2e8e313. Matches sf except where rings overlap: geo's odd-even rule keeps overlapping holes and nested holes as islands and drops overlapping parts, where sf's `valid_structure` subtracts and unions; full parity is out of scope for now. Always returns a multipolygon |
| 1.2 | Type checking: `st_geometry_type()`, `st_is()` | Done | No | `ga_geometry_type()`, `ga_is()`, `st_is()` method, masked `st_geometry_type()`; test-geometry-type.R, test-sf-compat.R; 09c73af, 69b2f82 |
| 1.3 | Collection extraction: `st_collection_extract()` | Done | No | `ga_collection_extract()`, `st_collection_extract()` method; test-collection-extract.R, test-sf-compat.R; 3817b50, 69b2f82 |
| 1.4 | Perimeter: `st_perimeter()` | Done | No | `ga_perimeter_euclidean()`; test-area.R; af06800. The masked `st_perimeter()` always calls `ga_perimeter_geodesic()`, see 3.3 |
| 1.5 | Coordinate comparison: `st_equals_exact()` | Done | No | `ga_equals_exact()` row by row and `ga_sparse_equals_exact()`, with `st_equals_exact()` attached by `mask_sf()`, sparse and dense; test-equals-exact.R; 39772ad, 8334a4d |
| 1.6 | Jittering: `st_jitter()` | Done | No | `ga_jitter()` with `amount` per row, sf's `factor` default, and `seed`, plus `st_jitter()` attached by `mask_sf()`; test-jitter.R; d4d94f5, c4fc8b8; empty points fixed in 24cdeaa. Planar only: sf's narrowing of x for longitude and latitude waits on 3.3. The offsets come from rand_pcg seeded from R's generator, so `set.seed()` repeats them but they never equal sf's, and the tests check sf's bounds rather than its values |
| 1.7 | Winding: `st_reverse()` | Done | No | `ga_reverse()`, `st_reverse()` method; test-winding.R, test-sf-compat.R; bb4c8a8, 69b2f82 |
| 1.8 | Areal interpolation: `st_interpolate_aw()` | Done | No | `ga_interpolate_aw()`: extensive or intensive per column, `include_non_intersected`, `na_rm`, and dasymetric `weights` as sf has them, plus tobler's `allocate_total`; a numeric vector in gives a float64 array, a table gives a struct array; an sf `st_interpolate_aw()` method for data frames with a GeoArrow geometry column; test-interpolate-aw.R; b401e78. Planar area only, see 3.3. geo's overlay agrees with GEOS to about 1e-8 relative per overlap |
| 1.9 | Bounding box: `st_bbox()` | Done | No | `ga_envelope()`, `st_bbox()` method; test-sf-compat.R; 1af87b4. Storing the box as an attribute is deferred |
| 1.10 | Polygon constructors | Done | No | `ga_point()` (z, m), `ga_linestring()`, `ga_multipoint()`, `ga_multilinestring()`, `ga_polygon()`, `ga_multipolygon()`, `ga_make_envelope()`, `ga_make_line_agg()`, `ga_make_polygon()`, multi types from `ga_collect_agg()`; test-construct.R, test-polygon.R, test-column-constructors.R, test-make-line-agg.R, test-make-polygon.R, test-aggregate.R; 0d37722, 90eb4c6, 14e731a, 1cef9cd, e4f1b7b, 6074e39, 712f10d; `ga_make_grid()` with `st_make_grid()` attached by `mask_sf()`, square grids only, test-make-grid.R, b9d8d4e. No sf wrapper for the others: sf's constructors take matrices |

## Milestone 2: Persistent spatial indexes and spatial sorting ($1,000, due 2027-02-01)

| # | Objective | Progress | Verified | Evidence |
|---|---|---|---|---|
| 2.1 | Persist an R-tree to disk | Done | No | `RTree$write()`, `RTree$read(mmap = )`; test-index-file.R; c9622b2 |
| 2.2 | Persist a k-d tree to disk | Done | No | `KDTree$write()`, `KDTree$read(mmap = )`; test-index-file.R; c9622b2 |
| 2.3 | Hilbert spatial sort | Done | No | `ga_hilbert_order()`; test-sort.R; 820a245 |
| 2.4 | Sort-tile-recursive spatial sort | Done | No | `ga_str_order()`; test-sort.R; 60f0832 |

## Milestone 3: sf compatibility and CRAN release ($1,500, due 2027-04-01)

| # | Objective | Progress | Verified | Evidence |
|---|---|---|---|---|
| 3.1 | Drop-in sf support through `mask_sf()` | In progress | No | `mask_sf()` and the S3 methods in `R/sf-compat.R`; test-sf-compat.R. The `mask_sf()` example fails: the masked `st_length()` rejects multipolygons |
| 3.2 | Tidy and rewrite docs, examples, and benchmarks | In progress | No | User-facing docs in active voice, 49d4138. README.md lags README.qmd. `bench/sf/equals-exact.R` benchmarks `st_equals_exact()` against sf, 8bf1189 |
| 3.3 | Geographic CRS: geodesic where possible, an error otherwise, never a silent planar result | In progress | No | `ga_is_longlat()` and the masked `st_is_longlat()` read a PROJJSON CRS through projjson and match `sf::st_is_longlat()`; test-is-longlat.R; 2dbb9a1 on the local branch crs-projjson. projjson's `is_geographic()` matches GDAL for all 13,510 CRSs in PROJ 9.5.1. Returns NA for an authority code or WKT. Nothing uses it yet: the masked `st_perimeter()` is always geodesic, `st_length()` and `st_area()` always planar |
| 3.4 | CRAN release | Not started | No | The 14 MB tarball needs a size exception |

## Milestone 4: GeoArrow and GeoParquet metadata ($1,000, due 2027-05-01)

| # | Objective | Progress | Verified | Evidence |
|---|---|---|---|---|
| 4.1 | Carry the CRS across the R and Rust boundary with no PROJ dependency | In progress | No | Every exported `ga_` function that returns geometries keeps the CRS content of its input for points, lines, and polygons; test-crs.R; 0af71d2, 4f4d942. On the local branch crs-projjson (2dbb9a1), projjson validates a PROJJSON CRS where it enters; the branch depends on an unpublished local projjson-rs (crs-inspection, 65be0d9) |
| 4.2 | Read and write the CRS as PROJJSON column metadata | In progress | No | On the local branch crs-projjson (2dbb9a1): `crs =` arguments store a valid PROJJSON CRS typed `projjson` and compact, `AUTHORITY:CODE` typed `authority_code`, anything else untyped; the masked `st_make_grid()` passes `$ProjJson`; `read_geojson()` writes OGC:CRS84 as PROJJSON; test-crs.R. `read_shapefile()` keeps the .prj WKT1 untyped, `read_flatgeobuf()` keeps what geoarrow-flatgeobuf gives, and there are no writers yet |
| 4.3 | GeoParquet 1.1 column metadata: `encoding`, `geometry_types`, `crs`, `bbox`, `covering`, `orientation`, `edges`, `epoch` | Not started | No | `covering` pairs with the 2.3 and 2.4 sorts |

## Definition of done

| # | Objective | Progress | Verified | Evidence |
|---|---|---|---|---|
| D.1 | Test every operation shared with sf against sf on every commit | In progress | No | The sf comparisons exist, but CI runs only pkgdown and set-version, not the tests. pkgdown installs sdf from GitHub through `Remotes`, 668332b |
| D.2 | Replicate the duckspatial benchmark on the package site | In progress | No | `bench/duckspatial/`, `vignettes/articles/duckspatial.qmd` |
| D.3 | Implement Apache Sedona SpatialBench | In progress | No | `bench/q1.R` to `bench/q12.R`, `vignettes/articles/spatialbench.qmd` |
| D.4 | Publish the joint blog post with the R Consortium | Not started | No | |
