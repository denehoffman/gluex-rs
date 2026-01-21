# gluex-lumi (Python)

Python bindings for the GlueX luminosity calculators. The package exposes `get_flux_histograms`
from the Rust crate and an entrypoint for the `gluex-lumi` CLI. Use `--plot=path.png` to save
a matplotlib figure when running from the CLI.

## Installation

Add to an existing Python project:

```bash
uv pip install gluex-lumi
```

or install as a CLI tool:

```bash
uv tool install gluex-lumi
```

To write a plot image from the CLI:

```bash
gluex-lumi --plot=flux.png --run f18=0 --bins=40 --coherent-peak --polarized \
  --rcdb=rcdb.sqlite --ccdb=ccdb.sqlite --min=8.0 --max=9.0
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
    exclude_runs=[50000, 50001],
)

luminosity = histos.tagged_luminosity.as_dict()
print("bin edges:", luminosity["edges"])
print("counts:", luminosity["counts"])
```

## License

Dual-licensed under Apache-2.0 or MIT.
