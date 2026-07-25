"""Tests for writing Python ``laddu`` generated batches through ``gluex``."""

from pathlib import Path

from gluex import generation as gluex_generation
from laddu import generation as laddu_generation


def _generated_batch(n_events: int = 2) -> laddu_generation.GeneratedBatch:
    beam = laddu_generation.GeneratedParticle.initial(
        "beam",
        laddu_generation.InitialGenerator.beam_with_fixed_energy(0.0, 9.0),
        laddu_generation.Reconstruction.stored(),
    ).with_species(laddu_generation.ParticleSpecies.code(22))
    target = laddu_generation.GeneratedParticle.initial(
        "target",
        laddu_generation.InitialGenerator.target(0.938_272_046),
        laddu_generation.Reconstruction.missing(),
    ).with_species(laddu_generation.ParticleSpecies.code(2212))
    kshort = laddu_generation.GeneratedParticle.stable(
        "kshort",
        laddu_generation.StableGenerator(0.497_614),
        laddu_generation.Reconstruction.stored(),
    ).with_species(laddu_generation.ParticleSpecies.code(310))
    recoil = laddu_generation.GeneratedParticle.stable(
        "recoil",
        laddu_generation.StableGenerator(0.938_272_046),
        laddu_generation.Reconstruction.stored(),
    ).with_species(laddu_generation.ParticleSpecies.code(2212))
    reaction = laddu_generation.GeneratedReaction.two_to_two(
        beam,
        target,
        kshort,
        recoil,
        laddu_generation.MandelstamTDistribution.exponential(1.0),
    )
    generator = laddu_generation.EventGenerator(
        reaction,
        seed=12345,
        storage=laddu_generation.GeneratedStorage.only(["beam", "kshort", "recoil"]),
    )
    return generator.generate_batch(n_events)


def test_generation_writer_consumes_laddu_bulk_columns(tmp_path: Path) -> None:
    batch = _generated_batch()
    writer = gluex_generation.GlueXHddmWriter(
        gluex_generation.GlueXHddmConfig(
            "beam",
            "target",
            run_number=90_000,
            first_event_number=7,
            random_seed=12345,
            vertex=(0.1, 0.2, 50.0),
        )
    )

    single_path = tmp_path / "single.hddm"
    assert writer.write_batch(batch, single_path) == 2
    assert single_path.stat().st_size > 0

    appended_path = tmp_path / "appended.hddm"
    next_event = writer.write_batch(batch, appended_path)
    assert writer.append_batch(batch, appended_path, next_event) == 4
    assert appended_path.stat().st_size > single_path.stat().st_size

    batches_path = tmp_path / "batches.hddm"
    writer.write_batches([batch, batch], batches_path)
    assert batches_path.stat().st_size == appended_path.stat().st_size
