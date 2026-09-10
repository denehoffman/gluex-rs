"""Explore typed run predicates and CCDB-only calibration retrieval."""

import argparse
from datetime import datetime, timezone

import gluex


def print_luminosity(gx: gluex.GlueX, run_query: gluex.RunQuery) -> None:
    resolved_runs = run_query.collect()
    luminosity = gx.workflows.luminosity(
        resolved_runs,
        reconstruction=gluex.ReconstructionSelection.latest(),
        edges=[8.0, 8.5, 9.0],
    ).collect()
    print('Tagged luminosity (1/pb):', luminosity.histograms.tagged_luminosity.counts)
    print('Luminosity run report:', luminosity.report)
    print('Luminosity procedure:', luminosity.provenance.procedure_version)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--rcdb')
    parser.add_argument('--ccdb')
    parser.add_argument('--as-of', help='ISO 8601 cutoff with timezone (e.g. 2019-01-01T00:00:00+00:00)')
    parser.add_argument('--variation', default='default')
    parser.add_argument('--refresh', action='store_true')
    parser.add_argument('--table', default='/TARGET/density')
    parser.add_argument('--run', type=int, action='append', default=None)
    args = parser.parse_args()
    gx = gluex.open(
        rcdb=args.rcdb or gluex.DISABLED,
        ccdb=args.ccdb or gluex.DISABLED,
    )
    selection = gluex.RunSelection.runs(args.run or [50685, 50697])
    run_query = None
    if gx.capabilities.rcdb:
        current = gx.runs.conditions['beam_current']
        minimum_current = 10.0
        run_query = gx.runs.select(selection).where(current > minimum_current)
        for chunk in run_query.stream(chunk_size=128):
            if chunk is None:
                continue
            print('Selected chunk:', chunk.numbers, 'complete:', chunk.report.complete)
        runs = run_query.collect()
        print('Selected:', runs.numbers)
        print('Unknown exclusions:', runs.report.unknown_runs)
        print('Run inputs:', runs.provenance)
        projected = gx.runs.select(selection).columns('beam_current', 'polarization_direction')
        values = projected.collect()
        print('Condition runs:', values.runs.numbers)
        print('Beam current (nA):', values.column('beam_current'))
        print('Missing cells:', values.report.missing_values)
    if gx.capabilities.ccdb:
        table = gx.calibrations[args.table]
        print('Columns:', [(column.name, column.value_type) for column in table.columns])
        calibration_runs = run_query if run_query is not None else selection
        query = table.for_runs(calibration_runs).with_variation(args.variation)
        if args.as_of:
            timestamp = datetime.fromisoformat(args.as_of)
            if timestamp.tzinfo is None:
                parser.error('--as-of requires an explicit timezone')
            query = query.as_of(timestamp.astimezone(timezone.utc))
        series = query.collect()
        for run, entry in series.items():
            print(run, entry.assignment_id, entry.constant_set_id)
            for name in entry.payload.columns:
                print(name, entry.payload.column(name))
        print('Missing assignments:', series.report.missing_runs)
        print('Calibration inputs:', series.provenance)
        if args.refresh:
            captured = query.provenance.as_of
            gx.refresh()
            print('Old query cutoff:', captured, query.provenance.as_of)
            print('Refreshed opening time:', gx.calibrations[args.table].for_runs(selection).provenance.as_of)

    if gx.capabilities.rcdb and gx.capabilities.ccdb and run_query is not None:
        print_luminosity(gx, run_query)


if __name__ == '__main__':
    main()
