# gluex-lumi (Python)

Python bindings for the GlueX luminosity calculators. The package exposes `Luminosity` and
`Context` classes from the Rust crate plus an entrypoint for the `gluex-lumi` CLI.

## Installation

Add to an existing Python project:

```bash
uv pip install gluex-lumi
```

or install as a CLI tool:

```bash
uv tool install gluex-lumi
```

## Example

```python
import gluex_lumi as lumi

edges = [7.5 + 0.05 * i for i in range(21)]
runs = [50002, 50003, 50004]
ctx = lumi.Context(
    runs,
    rest_version={"f18": None},  # uses current timestamp rather than REST version
    coherent_peak=True,
    exclude_runs=[50003],
)
lumi_client = lumi.Luminosity(rcdb="/data/rcdb.sqlite", ccdb="/data/ccdb.sqlite")
histos = lumi_client.fetch(edges, ctx)

luminosity = histos.tagged_luminosity.as_dict()
print("bin edges:", luminosity["edges"])
print("counts:", luminosity["counts"])
```

## License

Dual-licensed under Apache-2.0 or MIT.
