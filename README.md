# gluex-rs

Python tools for GlueX analysis, conditions data, luminosity calculations, and HDDM event generation. The package installs the `gluex` Python module and the matching `gluex` command-line program.

## Install

Use Python 3.11 or newer. With pip:

```bash
python -m pip install gluex-rs
```

Or add it to a uv project:

```bash
uv add gluex-rs
```

The package installs [laddu](https://github.com/denehoffman/laddu) for channel construction and generation. Confirm the installation:

```bash
python -c "import gluex; print(gluex.__version__)"
gluex --help
```

## Python

```python
import gluex

period = gluex.RunPeriod("f18")
print(period.min_run, period.max_run)
```

GlueX conditions and luminosity utilities are exposed through the `gluex.ccdb`, `gluex.rcdb`, and `gluex.lumi` modules. The command-line interface is useful for common one-off tasks:

```bash
gluex info runs f18
gluex info rest f18
gluex lumi --run f18=2 --rcdb rcdb.sqlite --ccdb ccdb.sqlite
```

## Event generation

Generation configurations are authored in Python. GlueX turns a Laddu channel, an optional model, its parameter values, and any additional scalar branches into a versioned JSON execution manifest. The JSON is an interchange artifact for `gluex gen check` and `gluex gen run`; users do not need to write or edit it.

This example creates \(\gamma p \to \pi^+ n\) and writes a model-less configuration:

```python
import laddu as ld
from gluex import generation

channel = ld.Channel(
    "gamma p -> pi+ n",
    edges=[
        ld.Edge(
            "beam",
            particle=ld.particles.PHOTON,
            initial_momentum=ld.InitialMomentum.uniform_energy(
                low=8.0,
                high=9.0,
                direction=[0.0, 0.0, 1.0],
            ),
        ),
        ld.Edge(
            "target",
            particle=ld.particles.PROTON,
            initial_momentum=ld.InitialMomentum.momentum([0.0, 0.0, 0.0]),
        ),
        ld.Edge("pi_plus", particle=ld.particles.PI_PLUS, output=True),
        ld.Edge("neutron", particle=ld.particles.NEUTRON, output=True),
    ],
    vertices=[
        ld.Vertex(
            "production",
            incoming=["beam", "target"],
            outgoing=["pi_plus", "neutron"],
            generation=ld.VertexProposal.t_exchange(
                incoming="beam",
                outgoing="pi_plus",
            ),
        ),
    ],
)

config = generation.GenerationConfig(channel)
config.write("generation.json")
```

`generation.config_json(channel)` remains available when a JSON string is more convenient. `GenerationConfig.write(...)` is preferred for complete scripts because the same object can configure and validate every generation option.

```bash
gluex gen check generation.json
gluex gen run generation.json --events 100000 --run-number 90000
```

The default seed is `0`, and this command writes `generation.hddm` beside the manifest. Use `--seed` or `--output` to override either default.

Users can also run a Monte Carlo generation in Python rather than using the CLI:

```python
report = config.run(
    "events.hddm",
    events=10_000,
    run_number=30_000,
    seed=1,
)
print(report["acceptance_rate"])
```

The optional model and additional scalar branches are configured in the same Python script. Parameters may be supplied by name or in `model.parameter_names` order:

```python
scale = ld.parameter("scale", initial=1.0)
model = ld.Model(scale * (ld.scalar("polarization") + 1.0))

config = generation.GenerationConfig(
    channel,
    model=model,
    parameters={"scale": 2.0},
    scalars={
        "polarization": generation.Scalar.uniform(-0.5, 0.5),
        "setting": generation.Scalar.fixed(7.0),
    },
    pilot_proposals=20_000,
    safety_scale=2.0,
)
config.add_scalar(
    "calibration",
    generation.Scalar.histogram(
        edges=[0.0, 1.0, 2.0],
        weights=[1.0, 3.0],
    ),
)
config.validate()
config.write("generation.json")
```

`Scalar.uniform(low, high)` samples on `[low, high)`. Scalar branches are available to Laddu expressions through `ld.scalar(name)` while the model is evaluated; fixed and histogram-backed scalar sources are also supported. Set `config.max_weight` if you have an independently known bound, or leave it unset to use the pilot estimate for a model-backed configuration.

### Transfer proposals

The production vertex above pairs the incoming `beam` with outgoing `pi_plus`. `t_exchange(...)` with no `slope` samples uniformly in the corresponding Mandelstam transfer \(t=(p_\gamma-p_{\pi^+})^2\). The range is the physical \(t\) interval for that event’s sampled beam energy and final-state masses, so it changes across the 8–9 GeV beam-energy range. It is not a fixed interval and it is not a separate uniform angular proposal.

To restrict the proposal, provide `t_min` and/or `t_max`; they are intersected with the physical interval. For example, use `t_min=-4.0, t_max=0.0` in `ld.VertexProposal.t_exchange(...)`. The example’s unconstrained uniform proposal uses the entire physical interval.

### Certified unweighting

For unit-model generation, this library uses a branch-and-bound envelope solver to obtain a certified finite upper bound for the proposal weight. This avoids tuning a pilot sample or relying on an observed maximum. The generation report records the certified envelope and proof statistics when available.

When a model is present and no manual bound is supplied, the generator estimates the envelope from `config.pilot_proposals` proposals and multiplies the observed maximum by `config.safety_scale`. These values can also be overridden for one
run with `--pilot-proposals`, `--max-weight`, and `--safety-scale`. If a pilot or manual envelope is exceeded, GlueX grows it, retrospectively thins buffered events, and prints a warning instead of failing.

## License

Dual-licensed under [Apache-2.0](LICENSE-APACHE) or [MIT](LICENSE-MIT).
