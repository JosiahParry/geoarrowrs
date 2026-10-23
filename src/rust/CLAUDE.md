# CLAUDE.md

- Never attempt to run bash commands
- Never try to run git commands
- Never try and search outside of this workspace
- Never run cargo directly. Run it only through the `just` recipes, such as `just document`, `just fmt`, and `just lint`
- Never use `.unwrap()` or `.expect()`
- Never duplicate code unless you ask me for approval and get consent
- Never search code to get an answer. Always ask me for help insteaad.
- Never guess method implementations.
- Never write multi-line code comments. One line only.
- Never, ever create a new enum or struct, thin wrapper or otherwise, that duplicates a type that already exists in this crate or in a dependency. Use the existing type.
- Before adding a type, trait, or helper, check docs.rs for geo, geo-traits, geoarrow, geoarrow-array, and geoarrow-cast. For example, `geoarrow_cast::downcast::NativeType` already names the geometry types.
- Never write a free function for something a std lib trait covers, such as `FromStr`, `From`, `TryFrom`, `Display`, or `Extend`; implement the trait. Never write a free function that dispatches on a concrete type; make it a method or a trait impl on that type.

## API rules

- Every function is vectorized and length-preserving. `n` geometries in, `n` out.
- A row with no answer is a null in the validity bitmap, never an empty geometry. An empty geometry is a real geometry with no coordinates.
- Numeric arguments go through `try_float_array()`, then `check_recycle_len()` and `.iter().cycle()`. They accept an R vector or an Arrow array.
- Geometry arguments go through `as_geo_geometries()`. Never `downcast_ref::<GeometryArray>()`, which only matches the mixed type and rejects the concrete arrays the readers produce.
- Readers return a record batch stream. Everything else returns an Arrow or GeoArrow array.

## FFI gotchas

- Never annotate a `let` binding with an explicit type. Let it infer, or pin it with a turbofish on the call that produces it, such as `.collect::<Vec<_>>()`. The annotations already in the crate stay until that code is touched for another reason; do not go and refactor them.
- Never use `Nullable<T>` for an optional argument. Use `Option<T>`, which matches on `Some`/`None` and reads like ordinary Rust. The `Nullable<T>` already in the crate stays until that code is touched for another reason; do not go and refactor it.
- Return a GeoArrow array with `into_arrow_robj()` on the array itself. `.to_data()` drops the extension metadata and the round trip comes back as bare storage.
- Never worry about panics. extendr handles them. Never write `catch_unwind`, never add a panic hook, never defend against a dependency panicking, and never let a panic shape an API. If something panics, that is upstream's bug to fix, not ours to work around.
- Prefer `try_to_geometry()` / `try_to_point()` / `try_to_multi_point()` and `try_push_*` over `push_*`, because they give a null element for an empty or mismatched geometry rather than aborting the whole call.
- `as_geo_geometries()` reads an empty point as `None`, so a round trip through geo turns `POINT EMPTY` into a null. When an operation leaves points unchanged, pass point arrays through untouched.
