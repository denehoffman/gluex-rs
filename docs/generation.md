# Standalone event generation

Generation consumes a versioned JSON manifest, validates it against the shipped schema, compiles the requested channel, and writes HDDM transactionally.

## Inspect and validate the format

```console
gluex gen schema --output generation.schema.json
gluex gen check examples/generation/piplus-neutron.json --json
```

Unknown fields and invalid particle/channel relationships are rejected before event generation.

## Generate events

```console
gluex gen run examples/generation/piplus-neutron.json \
  --events 10000 \
  --run-number 90000 \
  --seed 42 \
  --output events.hddm \
  --report events.report.json
```

The seed is deterministic. If `--output` is omitted, the manifest name is reused with an `.hddm` suffix. Existing output or report files require `--force`.

For model-free channels, the generator uses a certified rejection bound. Model-backed channels may estimate a bound from pilot proposals. The JSON report records the bound, proposal counts, updates, and proof metadata; keep it with the generated sample.

Use `--max-weight` only when you intentionally override automatic envelope construction. If a manual envelope is exceeded, generation grows it and emits a warning rather than silently biasing the sample.
