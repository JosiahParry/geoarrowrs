# CLAUDE.md

The Rust crate is at `src/rust` and has its own `CLAUDE.md`. Read it before
touching anything there.

## Workflow

Bindings are written in Rust and the R layer is generated. After changing any
`#[extendr]` function:

```
R -q -e "rextendr::document()"   # or: just document
```

That regenerates `R/extendr-wrappers.R`, `NAMESPACE`, and `man/`. Never edit
those by hand.

- `just lint` must pass before committing. Run `just fmt` first.
- `just test` runs testthat. Every new function needs tests.
- Commits are conventional (`feat:`, `fix:`, `chore:`). prek enforces this.
- Always update `NEWS.md`. Each entry is one sentence: what changed, nothing
  else. No benchmark numbers, no rationale, no mechanism explanation.

## Documentation

Roxygen lives in the Rust source above each `#[extendr]` function. Be
ruthlessly concise: a one-line title, two sentences of description, and
`@details` for anything longer. No em dash.

Write active voice with the function or the reader as the subject. Never
passive voice ("`y` is recycled against `x`") and never middle voice, where
the data acts on its own ("the output keeps the input's dimension", "a null
geometry gives `NA`"). Write "`ga_contains()` recycles `y` against `x`" and
"`ga_unsigned_area()` returns `NA` for a null geometry" instead. This applies to
everything a user reads: roxygen for exported functions, the README, and the
vignettes. Every exported function needs
`@examples` or `@examplesIf`, and they must actually run
(`devtools::run_examples()`).

## R code

Every capability is a `ga_` function first, the native path from Arrow arrays
to Arrow arrays. `st_*` functions and sdf methods are thin wrappers that only
call the `ga_` function, never a separate implementation.

`R/extendr-wrappers.R` is generated and excluded from air and jarl. Any
hand-written R goes in a new file under `R/` and is linted normally. Prefer
rlang standalone checks and `cli::cli_abort()` there rather than surfacing
Rust's error strings.

- Never use bare `try()`. Use `rlang::try_fetch()` with an `error` handler, so
  the condition is caught deliberately rather than turned into a sentinel
  object that has to be sniffed with `inherits()`.
- Never use `do.call()`. Splice with rlang instead: `rlang::inject(f(!!!args))`.

## Tests

- When sf has an equivalent, test against its output row by row. sf drops
  rows and splits multi part results, so compare per row rather than the
  whole vector.
- Assert nulls on the Arrow array, for example with `ga_geometry_type()` or
  `ga_is_empty()`, never after converting to sf, which turns a null into an
  empty geometry.

## Verifying

Never report a binding as working without running it from R. Type-checking
proves nothing about the FFI boundary, and several bugs this package has hit
compiled cleanly and then panicked or returned the wrong thing.
