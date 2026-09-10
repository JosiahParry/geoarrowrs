# Build changes to the document step

Written 2026-09-10. Read this before trusting the change, because the risky
part is a hardcoded list I derived on one machine.

## What was broken

`just document` failed at the link step:

```
Undefined symbols for architecture arm64:
  "_CFRelease", referenced from:
      ... iana_time_zone ...
  "_CFStringGetBytes", referenced from:
```

This blocked `rextendr::document()`, `devtools::test()`, and
`pkgload::load_all()` alike, because `pkgbuild` sets `ROXYGEN_PKG` for any
`compile_dll()`, not only for documenting. `R CMD INSTALL` was unaffected,
which is why `devtools::install()` kept working while everything else failed.

## Why

`src/Makevars.in` links `rust/document.c` into a **standalone executable**:

```make
$(CC) -o $(DOCUMENT) rust/document.c $(PKG_LIBS) -L"$(R_HOME)/lib$(R_ARCH)" -lR
```

`PKG_LIBS` is only `-L$(LIBDIR) -lgeoarrowrs`. That is enough for the shared
object, because R adds the system libraries itself when it links `.so`. An
executable gets no such help, so every system symbol the static lib depends on
has to be named explicitly.

The static lib does depend on them. Cargo says so on every build:

```
note: native-static-libs: -lR -liconv -framework CoreFoundation -lSystem -lc -lm
```

`-framework CoreFoundation` comes from `arrow -> chrono -> iana-time-zone`, so
it is not droppable the way the `flatgeobuf` TLS tree was.

## What I changed

Two files.

**`src/Makevars.in`** — one token added to the document rule:

```diff
-$(CC) -o $(DOCUMENT) rust/document.c $(PKG_LIBS) -L"$(R_HOME)/lib$(R_ARCH)" -lR && \
+$(CC) -o $(DOCUMENT) rust/document.c $(PKG_LIBS) -L"$(R_HOME)/lib$(R_ARCH)" -lR @DOCUMENT_LIBS@ && \
```

**`tools/config.R`** — a per-platform value for that token, plus one more
`gsub` in the existing chain:

```r
.document_libs <- switch(
  Sys.info()[["sysname"]],
  Darwin = "-liconv -framework CoreFoundation",
  Linux = "-lm -ldl -lpthread",
  ""
)
```

On this machine that generates:

```make
$(CC) -o $(DOCUMENT) rust/document.c $(PKG_LIBS) -L"$(R_HOME)/lib$(R_ARCH)" -lR -liconv -framework CoreFoundation && \
```

`just document` now runs clean through `document.c`.

## What I deleted

`src/rust/document.rs`, and with it the `[[bin]]` workaround I had been using
to generate wrappers while the C path was broken. Nothing references it:
`Cargo.toml` has no `[[bin]]` section committed, and no `.in`, `.toml`, `.R`,
or `.md` file mentions it. `rextendr::document()` may regenerate it; that is
harmless, since only `document.c` is linked.

## Where this could bite you

Ranked by how likely I think each is.

**1. The Linux list is a guess.** `-lm -ldl -lpthread` is the conventional set,
but I did not test on Linux and did not read a Linux `native-static-libs` line.
If the document step fails there, this is the first thing to look at. The
correct value is whatever `cargo build --lib` prints as
`native-static-libs`, minus `-lR`.

**2. The list is hardcoded, so it goes stale.** I read it off this dependency
graph on this machine. Add a crate that pulls in another system library and
the same class of failure returns with a different missing symbol. It is not
derived automatically.

Concretely: `-framework Security` **was** on that line until I dropped
`flatgeobuf`'s default features, which removed `reqwest` and `native-tls`. Put
an HTTP-using dependency back and you need `-framework Security` here again.

**3. Windows is untouched.** `src/Makevars.win.in` has the same rule but I did
not add the token to it. The `switch()` returns `""` for Windows anyway, so
behaviour there is unchanged rather than newly broken. If the document step
fails on Windows it was already failing.

## The better fix, not taken

Capture cargo's own output instead of hardcoding. The build already passes
`--print=native-static-libs`, so the true list is printed on every build. Have
`tools/config.R` run a probe build, parse that line, strip `-lR`, and
substitute the result. That would be self-maintaining across dependency
changes and correct on every platform without a lookup table.

I did not do it because it means running cargo during `configure`, which
lengthens the configure step and needs care about caching and CRAN policy.
Worth doing if this breaks a second time.

## How to revert

```
git checkout src/Makevars.in tools/config.R
git checkout HEAD -- src/rust/document.rs
```

Then regenerate `src/Makevars` with `R -q -e 'source("tools/config.R")'`, or
just let the next build do it.
