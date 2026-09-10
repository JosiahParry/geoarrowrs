# CLAUDE.md

- Never attempt to run bash commands
- Never try to run git commands
- Never try and search outside of this workspace
- Never try to run any cargo commands whatsoever
- Never use `.unwrap()` or `.expect()`
- Never duplicate code unless you ask me for approval and get consent
- Never search code to get an answer. Always ask me for help insteaad.
- Never guess method implementations.
- Never write multi-line code comments. One line only.

## API rules

- Every function is vectorized and length-preserving. `n` geometries in, `n` out.
- Numeric arguments go through `try_float_array()`, then `check_recycle_len()` and `.iter().cycle()`. They accept an R vector or an Arrow array.
- Geometry arguments go through `as_geo_geometries()`. Never `downcast_ref::<GeometryArray>()`, which only matches the mixed type and rejects the concrete arrays the readers produce.
- Readers return a record batch stream. Everything else returns an Arrow or GeoArrow array.

## FFI gotchas

- Return a GeoArrow array with `into_arrow_robj()` on the array itself. `.to_data()` drops the extension metadata and the round trip comes back as bare storage.
- Never worry about panics. extendr handles them. Never write `catch_unwind`, never add a panic hook, never defend against a dependency panicking, and never let a panic shape an API. If something panics, that is upstream's bug to fix, not ours to work around.
- Prefer `try_to_geometry()` / `try_to_point()` / `try_to_multi_point()` and `try_push_*` over `push_*`, because they give a null element for an empty or mismatched geometry rather than aborting the whole call.
