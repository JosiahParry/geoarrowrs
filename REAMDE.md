# geoarrowrs

`{geoarrowrs}` provides R bindings to the geoarrow-rs Rust crate. 

-----

## Notes

I like the `Table` struct in `geoarrow-rs` which generalizes the table structure of recordbatches. 

Should functions take the table structure? The ChunkedArray? Or both?

Right now, functionality is being written with normal geoarrow arrays but that doesn't feel good. We will not get parallelization via rayon that way. 



