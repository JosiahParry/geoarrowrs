# Changelog

## geoarrowrs (development version)

- Added
  [`ga_set_thread_pool()`](https://josiahparry.github.io/geoarrowrs/reference/ga_set_thread_pool.md),
  which hands the thread cap to the Rust side, and `.onLoad()` calls it
  with `getOption("geoarrowrs.thread_pool")`. The parallel paths used to
  read that option where they needed it, by evaluating R from inside the
  call, which is the R API reached from a place that has no business
  reaching it and cannot be relied on to be R’s own thread. The cap is
  now carried into the parallel helpers as an argument, and the option
  is read on load or whenever
  [`ga_set_thread_pool()`](https://josiahparry.github.io/geoarrowrs/reference/ga_set_thread_pool.md)
  is called.
- [`ga_sparse_knn()`](https://josiahparry.github.io/geoarrowrs/reference/ga_sparse_knn.md)
  and
  [`ga_sparse_dwithin()`](https://josiahparry.github.io/geoarrowrs/reference/ga_sparse_dwithin.md)
  take `metric`, one of `"euclidean"`, `"haversine"`, `"geodesic"`, or
  `"rhumb"`. On longitude and latitude the planar default measures
  degrees, which is not a distance: it ranks neighbours wrongly once
  rows are far apart or near a pole, and a radius in it is a different
  distance north to south than east to west. `"geodesic"` and
  `"haversine"` measure metres, and the bounding box each search grows
  is converted to match, generously enough to hold every candidate
  before the exact distance decides. `geo` defines those metrics between
  points alone, so asking for one with a polygon is an error rather than
  a planar number wearing a spherical name, and `"euclidean"` stays the
  default because it is the only one defined for every geometry type. It
  is also what Sedona, PostGIS and DuckDB’s `ST_DWithin()` measure, so
  it is what reproduces them.
- Added
  [`ga_filter()`](https://josiahparry.github.io/geoarrowrs/reference/ga_filter.md),
  which keeps the rows of one data frame that relate to any row of
  another. This is `ST_Filter()`, and the short form of
  `ga_join(left = FALSE)` where the columns of `y` are not wanted and a
  row of `x` matching several rows should still appear once.
- Added
  [`ga_sparse_pairs()`](https://josiahparry.github.io/geoarrowrs/reference/ga_sparse_pairs.md),
  which expands the list a sparse predicate returns into one row per
  pair. Arrow’s own `list_parent_indices()` and `list_flatten()` do this
  for the inner case; what they cannot do is put back a row for each row
  of `x` that matched nothing, which is what `left = TRUE` needs, so the
  whole expansion happens in one pass here.
- Added
  [`ga_cross_distance()`](https://josiahparry.github.io/geoarrowrs/reference/ga_cross_distance.md),
  which measures every row of `x` against every row of `y` rather than
  walking the two in lockstep, giving one list of `length(y)` distances
  per row. The result is rectangular, so the values are written straight
  into one flat buffer at each row’s own offset instead of appended
  through a builder, and each row is a rayon task. Ten thousand points
  against ten thousand, a hundred million haversine distances, takes
  0.6s against 35.8s for
  [`sf::st_distance()`](https://r-spatial.github.io/sf/reference/geos_measures.html)
  on the same machine. Where the distances are only wanted to pick a
  nearest row or a threshold,
  [`ga_sparse_knn()`](https://josiahparry.github.io/geoarrowrs/reference/ga_sparse_knn.md)
  and
  [`ga_sparse_dwithin()`](https://josiahparry.github.io/geoarrowrs/reference/ga_sparse_dwithin.md)
  answer that against an R-tree without forming the product at all.
- Added
  [`ga_sparse_knn()`](https://josiahparry.github.io/geoarrowrs/reference/ga_sparse_knn.md)
  and
  [`ga_knn_join()`](https://josiahparry.github.io/geoarrowrs/reference/ga_knn_join.md),
  a nearest neighbour search and the join over it.
  [`ga_sparse_knn()`](https://josiahparry.github.io/geoarrowrs/reference/ga_sparse_knn.md)
  returns the `k` rows of `y` nearest each row of `x`, nearest first,
  each with its distance, and
  [`ga_knn_join()`](https://josiahparry.github.io/geoarrowrs/reference/ga_knn_join.md)
  attaches those rows the way
  [`ga_join()`](https://josiahparry.github.io/geoarrowrs/reference/ga_join.md)
  attaches the ones that relate. Distance is measured between the
  geometries themselves rather than between their boxes, so a point
  joins to the polygon whose edge is nearest rather than to the one
  whose box is; the tree is used to bound the search and the answer is
  the same as comparing every pair. `RTree$neighbors()` still ranks by
  distance to the box, which is a candidate list rather than an answer.
- Materialising geometries runs in parallel, in rayon jobs of at least
  8192 rows, which is the dominant cost of every walk over a large
  array. The sparse predicates, the nearest neighbour search, both
  spatial indexes, and the pairwise distance and bearing functions all
  go through it, and the metric itself is threaded on top.
- [`ga_collect_agg()`](https://josiahparry.github.io/geoarrowrs/reference/ga_collect_agg.md)
  takes `sizes`, the number of rows in each group, and returns one
  collection per group instead of one for the whole array. This is the
  grouped form of the aggregate: the rows have to already be sorted by
  the grouping, and the run lengths are what a
  [`group_by()`](https://dplyr.tidyverse.org/reference/group_by.html)
  and `summarise(n = n())` over the same ordering give, so the keys come
  from there and only the geometry comes from here. A per group convex
  hull over 316k groups and 2.3M points takes about a second.
- Reading geometries splits into more, smaller rayon tasks, sized
  against the thread pool rather than at a fixed 8192 rows. A table of
  polygons can run from four coordinates to hundreds of thousands, so a
  few large tasks left cores idle behind whichever one drew the biggest
  geometries.
- Added
  [`ga_sparse_dwithin()`](https://josiahparry.github.io/geoarrowrs/reference/ga_sparse_dwithin.md),
  which finds the rows of `y` within a distance of each row of `x`. This
  is `ST_DWithin()`, and it is not the same as buffering `x` and
  intersecting: a buffer approximates its arcs with segments, so a row
  just inside the true radius can fall outside the buffer. Each box is
  grown by the radius before the tree is searched, so only the rows that
  could qualify are measured, and the radius is recycled so each row may
  have its own.
- A point on either side of a sparse predicate is answered from where
  the point lies rather than from a DE-9IM matrix. A point has an empty
  boundary and a zero dimensional interior, so whether it is inside, on,
  or outside the other geometry settles the whole matrix, and a ray cast
  replaces building a topology graph per pair. Beyond a handful of
  candidates the edges are indexed by the rows they span, using `geo`’s
  interval tree, so each test consults the edges near the point rather
  than all of them. Point in polygon over 156k polygons and 6M points
  went from 232s to 3.1s. The index is only used where its winding
  assumption holds, since it pools every ring into one winding number
  and so reads a hole wound like its exterior as inside it.
- The topological predicates node each geometry once rather than once
  per comparison, using `geo`’s prepared geometries. `relate()` builds a
  noded graph of both sides on every call, so a sparse predicate against
  a 500 vertex polygon was rebuilding that graph for every candidate
  row. The sparse predicates prepare each row of `x` before testing its
  candidates, and the pairwise predicates prepare `y` when it is a
  single geometry, which is the `ga_intersects(geometry, one_polygon)`
  case.
  [`ga_relate()`](https://josiahparry.github.io/geoarrowrs/reference/ga_relate.md)
  and the pairwise predicates are threaded at the same time.
- `RTree` takes arrays everywhere, matching `KDTree`.
  `$search(geometry)` replaces both the old `$query()` and the scalar
  `$search(xmin, ymin, xmax, ymax)`, and
  `$neighbors(geometry, k, max_distance)` replaces the scalar
  `$neighbors(x, y, ...)`, returning one list of rows per query row. A
  nearest neighbour join is now one call rather than a loop, which is
  what `$neighbors()` was missing.
- Added
  [`ga_collect_agg()`](https://josiahparry.github.io/geoarrowrs/reference/ga_collect_agg.md),
  which gathers a whole array into one geometry collection of length 1.
  Nothing is dissolved, so it is the counterpart to
  [`ga_unary_union()`](https://josiahparry.github.io/geoarrowrs/reference/ga_unary_union.md):
  two squares overlapping by half collect to area 8 and union to area 6.
  Both are aggregates rather than row by row operations, so they belong
  inside a
  [`summarise()`](https://dplyr.tidyverse.org/reference/summarise.html),
  and neither is registered as an Arrow kernel because a kernel sees one
  batch at a time and would aggregate each batch separately.
- [`ga_dist_euclidean_pairwise()`](https://josiahparry.github.io/geoarrowrs/reference/dist_pairwise.md)
  measures between geometries of any type, so the distance from a point
  to the nearest edge of a polygon is one call and a bare WKB column
  needs no cast first. The spherical and ellipsoidal metrics still take
  points, because `geo` defines them between points alone.
  [`ga_dist_hausdorff_pairwise()`](https://josiahparry.github.io/geoarrowrs/reference/ga_dist_hausdorff_pairwise.md)
  shares the same walk.
- Added
  [`ga_make_line()`](https://josiahparry.github.io/geoarrowrs/reference/ga_make_line.md),
  which joins each point of one array to the matching point of another
  as a two point linestring. The end point is recycled, so one
  destination pairs with every origin.
- Added
  [`ga_x()`](https://josiahparry.github.io/geoarrowrs/reference/ga_x.md)
  and
  [`ga_y()`](https://josiahparry.github.io/geoarrowrs/reference/ga_x.md),
  which read the coordinates of a point array as doubles. Both
  coordinate encodings work, so a point stored as a struct of two
  columns and one stored interleaved read the same. Anything that is not
  a single point gives `NA`.
- The distance and bearing functions recycle a length 1 `dest`, so
  `ga_dist_euclidean_pairwise(points, ga_xy(-111.76, 34.87))` measures
  every point against one location. This is the rule the topological
  predicates already followed. A mismatch that is neither length 1 nor
  the full length is still an error rather than a silent truncation.
- Added
  [`ga_join()`](https://josiahparry.github.io/geoarrowrs/reference/ga_join.md),
  which attaches the columns of one data frame to each row of another
  that it relates to spatially, and returns an Arrow table. A row
  matching several rows is repeated once per match, unmatched rows are
  kept with `NA` unless `left = FALSE`, shared column names are
  suffixed, and the geometry comes from `x`. The relationship is any of
  the sparse predicates, so points in polygons is
  `ga_join(sites, counties, ga_sparse_within)`. Both sides are taken in
  Arrow rather than subset in R, which is the difference between
  milliseconds and seconds once there are a few million rows.
- Added the sparse predicates
  [`ga_sparse_intersects()`](https://josiahparry.github.io/geoarrowrs/reference/sparse.md),
  [`ga_sparse_contains()`](https://josiahparry.github.io/geoarrowrs/reference/sparse.md),
  [`ga_sparse_contains_properly()`](https://josiahparry.github.io/geoarrowrs/reference/sparse.md),
  [`ga_sparse_within()`](https://josiahparry.github.io/geoarrowrs/reference/sparse.md),
  [`ga_sparse_covers()`](https://josiahparry.github.io/geoarrowrs/reference/sparse.md),
  [`ga_sparse_covered_by()`](https://josiahparry.github.io/geoarrowrs/reference/sparse.md),
  [`ga_sparse_touches()`](https://josiahparry.github.io/geoarrowrs/reference/sparse.md),
  [`ga_sparse_crosses()`](https://josiahparry.github.io/geoarrowrs/reference/sparse.md),
  [`ga_sparse_overlaps()`](https://josiahparry.github.io/geoarrowrs/reference/sparse.md),
  and
  [`ga_sparse_equals_topo()`](https://josiahparry.github.io/geoarrowrs/reference/sparse.md).
  Each returns the rows of `y` that relate to each row of `x`, rather
  than comparing the two row by row. `y` is indexed in an R-tree and
  only candidates are relate tested, so the cost follows the number of
  overlapping boxes rather than `length(x) * length(y)`. There is no
  `ga_sparse_disjoint()`: a bounding box cannot narrow disjointness, so
  the answer would be nearly every row.
- Added `KDTree`, a k-d tree over a point array. `$range(geometry)`
  finds the points inside each geometry’s box and `$within(geometry, r)`
  the points within `r` of each point. Both take an array and return one
  list of rows per row, so a whole distance band join is one call; a
  single lookup is an array of one via
  [`ga_xy()`](https://josiahparry.github.io/geoarrowrs/reference/ga_xy.md).
  `RTree` cannot answer a radius query directly, which is why this
  exists.
- Added `RTree`, a packed Hilbert R-tree over the bounding box of each
  geometry, built on the `geo-index` crate. `$query()` looks up a whole
  array at once and returns one list of candidate rows per element,
  which is the shape a spatial join needs; it accepts any GeoArrow array
  and reduces it to boxes itself. `$search()` takes a single box and
  `$neighbors()` the rows nearest a point. `$new()` takes `node_size`
  and a `sort` of `"hilbert"` or `"str"`. Queries return candidates to
  confirm with an exact predicate.
- Added
  [`ga_envelope()`](https://josiahparry.github.io/geoarrowrs/reference/ga_envelope.md),
  which returns the axis aligned bounding box of each geometry as a
  GeoArrow box array. Unlike
  [`ga_bounding_rect()`](https://josiahparry.github.io/geoarrowrs/reference/ga_bounding_rect.md)
  it passes a box array straight through instead of recomputing it, so
  it is safe to call before a query without paying for it twice.
- Added
  [`ga_dbscan()`](https://josiahparry.github.io/geoarrowrs/reference/ga_dbscan.md),
  [`ga_kmeans()`](https://josiahparry.github.io/geoarrowrs/reference/ga_kmeans.md),
  and
  [`ga_outlier_scores()`](https://josiahparry.github.io/geoarrowrs/reference/ga_outlier_scores.md),
  which cluster over the whole array and return one value per row, so a
  point array of 10,000 locations gives 10,000 labels. A row that is not
  a single point takes no part and comes back null. They are not
  registered as Arrow kernels, because a kernel sees one batch at a time
  and would cluster each batch separately. Added
  [`ga_simplify_idx()`](https://josiahparry.github.io/geoarrowrs/reference/ga_simplify_idx.md)
  and
  [`ga_simplify_vw_idx()`](https://josiahparry.github.io/geoarrowrs/reference/ga_simplify_idx.md)
  alongside, which return the coordinate positions simplification keeps
  rather than the simplified geometry.
- Added
  [`ga_as_point()`](https://josiahparry.github.io/geoarrowrs/reference/ga_as_point.md),
  [`ga_as_linestring()`](https://josiahparry.github.io/geoarrowrs/reference/ga_as_point.md),
  [`ga_as_polygon()`](https://josiahparry.github.io/geoarrowrs/reference/ga_as_point.md),
  [`ga_as_multipoint()`](https://josiahparry.github.io/geoarrowrs/reference/ga_as_point.md),
  [`ga_as_multilinestring()`](https://josiahparry.github.io/geoarrowrs/reference/ga_as_point.md),
  and
  [`ga_as_multipolygon()`](https://josiahparry.github.io/geoarrowrs/reference/ga_as_point.md),
  which cast a bare WKB column or any GeoArrow array to one named
  geometry type. Because the result type is fixed by the name rather
  than by the data, these are the only geometry producing casts Arrow
  can run inside a query, so a plain Parquet column can be turned into
  native GeoArrow once and computed on after that.
- Added
  [`ga_from_wkb()`](https://josiahparry.github.io/geoarrowrs/reference/ga_from_wkb.md),
  which reads a bare `binary` WKB column, the kind plain Parquet writes,
  into the narrowest GeoArrow type that fits it.
- Added
  [`ga_xy()`](https://josiahparry.github.io/geoarrowrs/reference/ga_xy.md),
  which pairs two numeric vectors or float64 arrays into a GeoArrow
  point array. Either argument may be length 1 and is recycled, a row
  where either coordinate is `NA` gives a null point, and `crs` is
  recorded in the array metadata verbatim without reprojecting anything.
- Added
  [`register_geoarrow_udfs()`](https://josiahparry.github.io/geoarrowrs/reference/register_geoarrow_udfs.md),
  which registers geoarrowrs functions as Arrow scalar kernels so they
  run inside `dplyr` verbs on a `Table` or `Dataset` rather than pulling
  the geometry into R. It registers for a CRS rather than for a table:
  one call covers every GeoArrow geometry type, and functions taking
  numeric or option arguments, such as
  [`ga_simplify()`](https://josiahparry.github.io/geoarrowrs/reference/ga_simplify.md),
  [`ga_densify()`](https://josiahparry.github.io/geoarrowrs/reference/ga_densify.md),
  and
  [`ga_buffer()`](https://josiahparry.github.io/geoarrowrs/reference/ga_buffer.md),
  are registered alongside the geometry-only ones. 101 functions in
  total.
- Loading the package registers the Arrow kernels for CRS-less data
  automatically, so `mutate(a = ga_unsigned_area(geometry))` works on a
  `Table` or `Dataset` with no setup. Data carrying a CRS still needs
  `register_geoarrow_udfs(crs = ...)`. Set
  `options(geoarrowrs.register_udfs = FALSE)` to skip it and avoid
  loading arrow at all.
- Registered Arrow kernels accept a bare `binary` or `large_binary` WKB
  column, the kind plain Parquet writes, so
  `mutate(d = ga_dist_euclidean_pairwise(pickup, dropoff))` runs on a
  Dataset without converting the column first. A geometry result from a
  WKB input comes back as WKB, since the kernel cannot declare a type
  that depends on what the column turns out to hold.
- Added
  [`ga_line_intersection()`](https://josiahparry.github.io/geoarrowrs/reference/ga_line_intersection.md)
  and
  [`ga_self_intersections()`](https://josiahparry.github.io/geoarrowrs/reference/ga_self_intersections.md),
  the latter using the Bentley-Ottmann sweep to locate exactly where a
  geometry crosses itself.
- Added
  [`ga_affine_transform()`](https://josiahparry.github.io/geoarrowrs/reference/ga_affine_transform.md),
  the general form behind the other affine ops, taking six recyclable
  coefficients so a different transform can apply to every row.
- Added
  [`ga_coords()`](https://josiahparry.github.io/geoarrowrs/reference/ga_coords.md),
  [`ga_exterior_coords()`](https://josiahparry.github.io/geoarrowrs/reference/ga_coords.md),
  [`ga_n_coords()`](https://josiahparry.github.io/geoarrowrs/reference/ga_n_coords.md),
  and
  [`ga_lines()`](https://josiahparry.github.io/geoarrowrs/reference/ga_lines.md)
  for walking a geometry’s vertices and segments, plus
  [`ga_is_valid()`](https://josiahparry.github.io/geoarrowrs/reference/validation.md)
  and
  [`ga_validation_error()`](https://josiahparry.github.io/geoarrowrs/reference/validation.md).
- Added
  [`ga_voronoi_cells()`](https://josiahparry.github.io/geoarrowrs/reference/ga_voronoi_cells.md)
  and
  [`ga_voronoi_edges()`](https://josiahparry.github.io/geoarrowrs/reference/ga_voronoi_edges.md),
  returning one multipolygon of cells or multilinestring of edges per
  input geometry. Both take a `clip` mode and an optional `boundary`
  polygon to cut the diagram to a study area.
- Added
  [`ga_orient()`](https://josiahparry.github.io/geoarrowrs/reference/ga_orient.md),
  [`ga_winding_order()`](https://josiahparry.github.io/geoarrowrs/reference/ga_winding_order.md),
  [`ga_is_ccw()`](https://josiahparry.github.io/geoarrowrs/reference/ga_is_ccw.md),
  and
  [`ga_is_cw()`](https://josiahparry.github.io/geoarrowrs/reference/ga_is_ccw.md).
  A multi part geometry reports a winding only when all of its parts
  agree.
- Added the topological predicates
  [`ga_contains()`](https://josiahparry.github.io/geoarrowrs/reference/topology.md),
  [`ga_contains_properly()`](https://josiahparry.github.io/geoarrowrs/reference/topology.md),
  [`ga_within()`](https://josiahparry.github.io/geoarrowrs/reference/topology.md),
  [`ga_covers()`](https://josiahparry.github.io/geoarrowrs/reference/topology.md),
  [`ga_covered_by()`](https://josiahparry.github.io/geoarrowrs/reference/topology.md),
  [`ga_intersects()`](https://josiahparry.github.io/geoarrowrs/reference/topology.md),
  [`ga_disjoint()`](https://josiahparry.github.io/geoarrowrs/reference/topology.md),
  [`ga_touches()`](https://josiahparry.github.io/geoarrowrs/reference/topology.md),
  [`ga_crosses()`](https://josiahparry.github.io/geoarrowrs/reference/topology.md),
  [`ga_overlaps()`](https://josiahparry.github.io/geoarrowrs/reference/topology.md),
  and
  [`ga_equals_topo()`](https://josiahparry.github.io/geoarrowrs/reference/topology.md),
  plus
  [`ga_relate()`](https://josiahparry.github.io/geoarrowrs/reference/ga_relate.md)
  for the DE-9IM string,
  [`ga_dimension()`](https://josiahparry.github.io/geoarrowrs/reference/ga_dimension.md),
  [`ga_boundary_dimension()`](https://josiahparry.github.io/geoarrowrs/reference/ga_dimension.md),
  [`ga_is_empty()`](https://josiahparry.github.io/geoarrowrs/reference/ga_is_empty.md),
  and
  [`ga_coordinate_position()`](https://josiahparry.github.io/geoarrowrs/reference/ga_coordinate_position.md).
- Added
  [`ga_boolean_intersection()`](https://josiahparry.github.io/geoarrowrs/reference/boolean_ops.md),
  [`ga_boolean_union()`](https://josiahparry.github.io/geoarrowrs/reference/boolean_ops.md),
  [`ga_boolean_difference()`](https://josiahparry.github.io/geoarrowrs/reference/boolean_ops.md),
  [`ga_boolean_xor()`](https://josiahparry.github.io/geoarrowrs/reference/boolean_ops.md),
  and
  [`ga_unary_union()`](https://josiahparry.github.io/geoarrowrs/reference/ga_unary_union.md).
- Added
  [`ga_closest_point()`](https://josiahparry.github.io/geoarrowrs/reference/ga_closest_point.md),
  [`ga_closest_point_haversine()`](https://josiahparry.github.io/geoarrowrs/reference/ga_closest_point.md),
  [`ga_interior_point()`](https://josiahparry.github.io/geoarrowrs/reference/ga_interior_point.md),
  [`ga_is_convex()`](https://josiahparry.github.io/geoarrowrs/reference/ga_is_convex.md),
  and
  [`ga_line_locate_point()`](https://josiahparry.github.io/geoarrowrs/reference/ga_line_locate_point.md).
  [`ga_interior_point()`](https://josiahparry.github.io/geoarrowrs/reference/ga_interior_point.md)
  always lands on the geometry, unlike
  [`ga_centroid()`](https://josiahparry.github.io/geoarrowrs/reference/ga_centroid.md).
- Added
  [`read_shapefile()`](https://josiahparry.github.io/geoarrowrs/reference/read_shapefile.md),
  [`read_geojson()`](https://josiahparry.github.io/geoarrowrs/reference/read_geojson.md),
  and
  [`read_flatgeobuf()`](https://josiahparry.github.io/geoarrowrs/reference/read_flatgeobuf.md),
  each returning a record batch stream of the attribute columns followed
  by a `geometry` column.
  [`read_flatgeobuf()`](https://josiahparry.github.io/geoarrowrs/reference/read_flatgeobuf.md)
  streams and takes an optional `bbox` that uses the file’s spatial
  index.
- Added
  [`ga_cast_geometry()`](https://josiahparry.github.io/geoarrowrs/reference/ga_cast_geometry.md)
  and
  [`ga_downcast_geometry()`](https://josiahparry.github.io/geoarrowrs/reference/ga_downcast_geometry.md)
  for converting between GeoArrow geometry types, and
  [`ga_explode()`](https://josiahparry.github.io/geoarrowrs/reference/ga_explode.md)/[`ga_flatten()`](https://josiahparry.github.io/geoarrowrs/reference/ga_flatten.md)
  for splitting multi-part geometries into their parts and collapsing
  them again.
  [`ga_explode()`](https://josiahparry.github.io/geoarrowrs/reference/ga_explode.md)
  preserves length, returning one array of parts per row.
- Added
  [`ga_triangulate_earcut()`](https://josiahparry.github.io/geoarrowrs/reference/ga_triangulate_earcut.md)
  and
  [`ga_triangulate_delaunay()`](https://josiahparry.github.io/geoarrowrs/reference/ga_triangulate_delaunay.md),
  returning one multipolygon of triangles per input geometry.
- Added
  [`ga_convex_hull()`](https://josiahparry.github.io/geoarrowrs/reference/ga_convex_hull.md),
  [`ga_concave_hull()`](https://josiahparry.github.io/geoarrowrs/reference/ga_concave_hull.md),
  and
  [`ga_extremes()`](https://josiahparry.github.io/geoarrowrs/reference/ga_extremes.md),
  plus the affine operations
  [`ga_translate()`](https://josiahparry.github.io/geoarrowrs/reference/ga_translate.md),
  [`ga_rotate_around_centroid()`](https://josiahparry.github.io/geoarrowrs/reference/ga_rotate_around_centroid.md),
  [`ga_rotate_around_center()`](https://josiahparry.github.io/geoarrowrs/reference/ga_rotate_around_center.md),
  [`ga_scale_xy()`](https://josiahparry.github.io/geoarrowrs/reference/ga_scale_xy.md),
  [`ga_skew()`](https://josiahparry.github.io/geoarrowrs/reference/ga_skew.md),
  and
  [`ga_skew_xy()`](https://josiahparry.github.io/geoarrowrs/reference/ga_skew_xy.md).
- Added
  [`ga_densify()`](https://josiahparry.github.io/geoarrowrs/reference/ga_densify.md),
  [`ga_interpolate_point()`](https://josiahparry.github.io/geoarrowrs/reference/ga_interpolate_point.md),
  [`ga_point_at_distance_between()`](https://josiahparry.github.io/geoarrowrs/reference/interpolate_between.md),
  [`ga_point_at_ratio_between()`](https://josiahparry.github.io/geoarrowrs/reference/interpolate_between.md),
  [`ga_points_along_line()`](https://josiahparry.github.io/geoarrowrs/reference/ga_points_along_line.md),
  [`ga_dest_euclidean()`](https://josiahparry.github.io/geoarrowrs/reference/destination.md),
  [`ga_dest_haversine()`](https://josiahparry.github.io/geoarrowrs/reference/destination.md),
  and
  [`ga_dest_geodesic()`](https://josiahparry.github.io/geoarrowrs/reference/destination.md).
- Added
  [`ga_to_degrees()`](https://josiahparry.github.io/geoarrowrs/reference/ga_to_degrees.md)
  and
  [`ga_to_radians()`](https://josiahparry.github.io/geoarrowrs/reference/ga_to_radians.md).
- Casting between geometry types runs in parallel, in rayon jobs of at
  least 8192 rows. Parsing WKB is most of the cost of reading a plain
  Parquet geometry column, and Arrow cannot thread a user-defined
  function because it has to call back into R on the main thread, so the
  parallelism has to live inside the Rust. On ten cores a 3M row WKB to
  point cast goes from 2.0s to 0.32s, and a distance over 6M rows from
  9.6s to 3.3s.
- The parallel paths use every core by default. Set
  `options(geoarrowrs.thread_pool = n)` for a cap, which takes effect
  immediately rather than at load. Under `R CMD check` the cap defaults
  to two, since CRAN sets `_R_CHECK_LIMIT_CORES_` and asks for no more
  than that; `OMP_THREAD_LIMIT` is honoured as a ceiling too. Setting
  the option yourself overrides both.
- Every function is vectorized and length preserving: `n` geometries in,
  `n` out. The pairwise distance and bearing functions error on a length
  mismatch rather than truncating to the shorter side.
- Numeric arguments accept a plain R numeric vector as well as an Arrow
  array, so `ga_simplify(x, 0.01)` works without wrapping the value.
  Length 1 is recycled.
- Geometry arguments accept any GeoArrow array rather than only a mixed
  `geometry` array, so the concrete arrays the readers produce work
  directly.
