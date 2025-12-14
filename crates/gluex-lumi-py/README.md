# gluex-lumi (Python)

Python bindings for the GlueX luminosity calculators. The package exposes `get_flux_histograms`
from the Rust crate and an entrypoint for the `gluex-lumi` CLI. It also adds a "plot" subcommand to the CLI.

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
histos = lumi.get_flux_histograms(
    {"f18": None}, # uses current timestamp rather than REST version
    edges,
    coherent_peak=True,
    rcdb="/data/rcdb.sqlite",
    ccdb="/data/ccdb.sqlite",
)

luminosity = histos.tagged_luminosity.as_dict()
print("bin edges:", luminosity["edges"])
print("counts:", luminosity["counts"])
```

## License

Dual-licensed under Apache-2.0 or MIT.
