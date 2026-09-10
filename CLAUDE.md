# CLAUDE.md

The Rust crate is at `src/rust` and has its own `CLAUDE.md`. Read it
before touching anything there.

## Workflow

Bindings are written in Rust and the R layer is generated. After
changing any `#[extendr]` function:

    R -q -e "rextendr::document()"   # or: just document

That regenerates `R/extendr-wrappers.R`, `NAMESPACE`, and `man/`. Never
edit those by hand.

- `just lint` must pass before committing. Run `just fmt` first.
- `just test` runs testthat. Every new function needs tests.
- Commits are conventional (`feat:`, `fix:`, `chore:`). prek enforces
  this.
- Always update `NEWS.md`.

## Documentation

Roxygen lives in the Rust source above each `#[extendr]` function. Be
ruthlessly concise: a one-line title, two sentences of description, and
`@details` for anything longer. No em dash. Every exported function
needs `@examples` or `@examplesIf`, and they must actually run
(`devtools::run_examples()`).

## R code

`R/extendr-wrappers.R` is generated and excluded from air and jarl. Any
hand-written R goes in a new file under `R/` and is linted normally.
Prefer rlang standalone checks and
[`cli::cli_abort()`](https://cli.r-lib.org/reference/cli_abort.html)
there rather than surfacing Rust’s error strings.

- Never use bare [`try()`](https://rdrr.io/r/base/try.html). Use
  [`rlang::try_fetch()`](https://rlang.r-lib.org/reference/try_fetch.html)
  with an `error` handler, so the condition is caught deliberately
  rather than turned into a sentinel object that has to be sniffed with
  [`inherits()`](https://rdrr.io/r/base/class.html).
- Never use [`do.call()`](https://rdrr.io/r/base/do.call.html). Splice
  with rlang instead: `rlang::inject(f(!!!args))`.

## Verifying

Never report a binding as working without running it from R.
Type-checking proves nothing about the FFI boundary, and several bugs
this package has hit compiled cleanly and then panicked or returned the
wrong thing.
