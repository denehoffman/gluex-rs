# Command-line guide

The `gluex` executable exposes reference information, luminosity calculation, and standalone event generation.

## Inspect run periods and REST versions

```console
gluex info runs
gluex info runs 2018-08
gluex info rest 2018_08
```

`info runs` prints names and numeric ranges. Coherent-energy windows are database assignments, so they are not presented as static run-period metadata.

## Calculate luminosity

```console
gluex lumi \
  --run 2018-08=2 \
  --bins 60 --min 6 --max 12 \
  --rcdb /data/rcdb.sqlite \
  --ccdb /data/ccdb.sqlite \
  --coherent-peak
```

Repeat `--run` to combine periods. A value after `=` is the REST revision. Omitting it uses the session's current CCDB cutoff. Output is JSON containing tagged flux, TAGM flux, TAGH flux, and tagged luminosity histograms.

`--coherent-peak` resolves `/PHOTON_BEAM/coherent_energy` per run from the supplied CCDB. `--polarized` additionally restricts runs using RCDB beam conditions.

## Environment configuration

`RCDB_CONNECTION` and `CCDB_CONNECTION` supply default paths when the corresponding flags are omitted.

## Discover generation commands

```console
gluex gen schema
gluex gen check examples/generation/piplus-neutron.json --json
gluex gen run --help
```

See [generation](generation.md) before producing events.
