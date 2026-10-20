# Event file I/O

The Python readers return `laddu.Dataset` objects. The FSRoot and Pawian writers accept datasets, including selected or reweighted views, and return laddu's `WriteResult` with output paths and event counts. AmpTools currently has a reader only.

```python
import gluex

data = gluex.io.read_pawian(
    "events.dat", particles=3, weights=True, polarization=True,
)
result = gluex.io.write_fsroot(data, "events.root", particles=3)
print(result.paths, result.events)
```

For repeated conversions, construct a reusable laddu format specification. These specifications also work with `laddu.io.convert` and `Dataset.write_to`:

```python
import laddu

pawian = gluex.io.pawian_format(particles=3, weights=True)
fsroot = gluex.io.fsroot_format(particles=3, prefix="kin_")
result = laddu.io.convert(
    "events.dat", "events.root", reader=pawian, writer=fsroot,
)
data = fsroot.read("events.root", cache="streaming")
data.write_to(gluex.io.pawian_sink("copy.dat", particles=3, weights=True))
```

`gluex.io.amptools_format(particles=3, pol_in_beam=True)` provides a reusable AmpTools reader. This option decodes beam `px` and `py` using the Zlm polarization convention. Normal mode preserves beam momentum.

FSRoot and AmpTools use ROOT trees named `nt` and `kin` by default. FSRoot momentum and weight branches must be `Double_t`; native AmpTools momentum branches must be `Float_t`. Pawian has explicit options for event weights, polarization, momentum order, and GeV or MeV units. Its input reader infers a z-axis photon beam and stationary proton target and checks momentum balance. Polarization angles are radians.

laddu 0.28 manages distributed reads and writes through its `execution` and `output` options:

```python
execution = laddu.Execution("cpu", partitioning="contiguous")
result = pawian.write(data, "output.dat", execution=execution, output="auto")
```

With an MPI-enabled laddu build, `auto` writes one file per rank; `output="single"` gathers a bounded stream to rank zero. These readers use laddu's generic row or contiguous partitioning, which scans the input on each rank. Native file-group partitioning is not advertised. AmpTools and FSRoot sources currently load a complete file per traversal, and the FSRoot sink buffers rows until the ROOT file is finished.
