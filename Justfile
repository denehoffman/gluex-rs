set dotenv-load
venv := ".venv"
bin := ".venv/bin"
python := ".venv/bin/python"
export CARGO_INCREMENTAL := "1"
export UV_PYTHON := ".venv/bin/python"
set quiet

default:
    just --choose

create-venv:
    if [ ! -d "{{venv}}" ]; then uv venv {{venv}} --python=3.14; fi

build-rust:
    cargo build --release

build-python: create-venv
    uvx --with "maturin[patchelf]>=1.11,<2" maturin develop --release --uv --generate-stubs

build: build-rust build-python

lint-rust:
    cargo clippy

lint-python:
    ruff check --fix
    ruff format
    ty check

lint: lint-rust lint-python

clean:
    cargo clean

test-rust:
    cargo test --release

test-python: build-python
    pytest python/tests

test: test-rust test-python

docs:
    cargo doc
