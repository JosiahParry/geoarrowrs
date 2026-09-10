Vectorized geospatial algorithms over GeoArrow arrays, using Rust
bindings to the geoarrow-rs and GeoRust projects. Reads shapefiles,
GeoJSON, and FlatGeobuf into Arrow, computes areas, lengths, distances,
topological predicates, and set operations, and registers those
functions with Arrow so they run inside dplyr pipelines on a Table or
Dataset.
