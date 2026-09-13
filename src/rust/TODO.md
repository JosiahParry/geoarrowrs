# TODO

 If `ga_unary_union()` ever becomes a bottleneck, the usual fix is to sort by bounding box and union the closest first rather than folding pairwise, which is what GEOS `CascadedUnion` does. `geo` takes a different route: `unary_union` flattens every ring from every geometry into one set and runs a single sweep over all of them, so it already avoids the quadratic pairwise fold, and a spatial pre-sort will not help a sweep that orders its own events. Measure before changing anything here.
- When retrieving chunked arrays, always return chunked arrays
- Use rayon wih a minimum chunk size of 4096
- Handle size checking and iterator cycling

## Chunked Arrays

- Need to first implement this in arrow-extendr using arrow ChunkedArray and nanoarrow-vctr


## CRS

- need to figure out extracting the crs from the schema metadata 
- need to figure out setting the proper crs
