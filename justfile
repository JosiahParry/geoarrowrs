default:
    just --list

fmt:
    air format R tests
    cargo fmt --manifest-path src/rust/Cargo.toml

lint:
    jarl check R tests && air format --check R tests
    cargo fmt --manifest-path src/rust/Cargo.toml --check && cargo clippy --manifest-path src/rust/Cargo.toml -- -D warnings

check:
    cargo check --manifest-path src/rust/Cargo.toml

# stage everything and commit, e.g. `just commit feat "add read_geojson()"`.
# `type` takes an optional scope: `just commit "fix(cast)" "promote linestrings"`
commit type message:
    git add -A && git commit -m '{{ type }}: {{ message }}'

test:
    R -q -e "devtools::test()"

document:
    R -q -e "rextendr::document()"

install:
    R -q -e "devtools::install()"
