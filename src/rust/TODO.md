# TODO

- `ga_collect_agg()` should take `by`, a column to group on, not just `sizes`. Grouping is currently the caller's job and costs more than the aggregate does: dissolving 3M points is 1.25s, of which roughly half is sorting and marshalling in R and Acero before the kernel starts. SedonaDB does the same operation in 0.17s with `ST_Collect_Agg` because the group by and the collect happen in one engine with no conversion. This is the only operation in `bench/duckspatial` where we are not the fastest. Note that this path does no geometry work at all, it only concatenates, so the cost is the grouping and nothing else.
- If `ga_unary_union()` ever becomes a bottleneck, the usual fix is to sort by bounding box and union the closest first rather than folding pairwise, which is what GEOS `CascadedUnion` does. `geo` takes a different route: `unary_union` flattens every ring from every geometry into one set and runs a single sweep over all of them, so it already avoids the quadratic pairwise fold, and a spatial pre-sort will not help a sweep that orders its own events. Measure before changing anything here.
- When retrieving chunked arrays, always return chunked arrays
- Use rayon wih a minimum chunk size of 4096
- Handle size checking and iterator cycling

## Chunked Arrays

- Need to first implement this in arrow-extendr using arrow ChunkedArray and nanoarrow-vctr


## CRS

- need to figure out extracting the crs from the schema metadata 
- need to figure out setting the proper crs
