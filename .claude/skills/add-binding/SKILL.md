---
name: add-binding
description: Add a new geo algorithm binding to geoarrowrs, wiring a geo or geoarrow trait through to an exported R function. Use whenever implementing an item from src/rust/README.md's checklist, adding an #[extendr] function to the crate, or when a binding compiles but is not reachable from R.
---

# Adding a binding

Every binding is a Rust `#[extendr]` function; the R layer is generated. The
steps below are the ones that actually go wrong.

## 1. Check what upstream gives you

Before writing anything, confirm the trait exists and on which types:

```
G=~/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/geo-0.33.1
grep -n "pub trait Foo" -A 8 $G/src/algorithm/foo.rs
grep -n "^impl.*Foo for" $G/src/algorithm/foo.rs
```

Traits are often narrower than they look. `TriangulateEarcut` is `Polygon`
only. `ConcaveHull` is not implemented for `Geometry`. `LinesIter` is not
implemented for `Geometry`, so constrained Delaunay needs a per-variant match.
Check before designing around a blanket impl that isn't there.

Also check whether geoarrow already has it. `geoarrow-cast` covers geometry
type casting; `geoarrow-flatgeobuf` has a full reader. But `geoarrow-geojson`
0.8 is write-only, and `Explode` and `GeoTableBuilder` were dropped in the 0.8
rewrite. Code in `geoarrow-old` will not compile against 0.8.

## 2. Write the module

Signature shape:

```rust
#[extendr]
fn thing(geometry: Robj, param: Robj) -> extendr_api::Result<Robj> {
    let p = try_float_array(param, "param")?;
    let chunks = as_geometry_chunks(geometry)?;
    let n: usize = chunks.iter().map(|c| c.len()).sum();
    check_recycle_len(p.len(), n, "param")?;

    let metadata = chunks[0].data_type().metadata().clone();
    let mut bldr = PolygonBuilder::new(PolygonType::new(Dimension::XY, metadata));

    for chunk in &chunks {
        for (geom, v) in as_geo_geometries(chunk.as_ref())?
            .into_iter()
            .zip(p.iter().cycle())
        {
            match (geom, v) {
                (Some(g), Some(v)) => bldr.push_polygon(Some(&g.thing(v)))
                    .map_err(|e| Error::Other(e.to_string()))?,
                _ => bldr.push_polygon(None::<&Polygon<f64>>)
                    .map_err(|e| Error::Other(e.to_string()))?,
            }
        }
    }

    bldr.finish().into_arrow_robj()
}
```

For type-preserving operations, dispatch with the `as_*_chunks` helpers instead,
as `simplify` and the affine ops do.

## 3. Register it, in both places

This is the step that gets missed. A module needs an `extendr_module!` block
**and** an entry in `lib.rs`:

```rust
// src/rust/src/thing/mod.rs
extendr_module! { mod thing; fn thing; }

// src/rust/src/lib.rs -- both of these
pub(crate) mod thing;
extendr_module! { mod geoarrowrs; use thing; }
```

Miss either and the code compiles, tests pass, and no R user can call it.
`densify`, `interpolate_point`, and three `dest_*` variants shipped this way.
A missing `@export` does the same thing more quietly.

## 4. Generate and verify

```
just document                       # rextendr::document()
grep thing NAMESPACE                # it must appear here
```

Then run it from R against a real array. Do not stop at `cargo check`:

```r
g <- as.data.frame(read_shapefile(system.file("shape/nc.shp", package = "sf")))$geometry
thing(g, 0.1)
```

## 5. Test

`tests/testthat/test-thing.R`. Cover: correct values, length preserved,
recycling and a wrong-length error, a null or empty geometry, and a concrete
array type (a multipolygon from `read_shapefile()`, not just a mixed one).

Empty geometries and concrete array types are where the real bugs have been.

## 6. Finish

Update `NEWS.md`, tick the item in `src/rust/README.md`, and confirm
`just lint` passes.
