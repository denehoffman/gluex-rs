"""Explore typed run predicates and CCDB-only calibration retrieval."""

import argparse

import gluex


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--rcdb')
    parser.add_argument('--ccdb')
    parser.add_argument('--table', default='/TARGET/density')
    parser.add_argument('--run', type=int, action='append', default=None)
    args = parser.parse_args()
    gx = gluex.open(
        rcdb=args.rcdb or gluex.DISABLED,
        ccdb=args.ccdb or gluex.DISABLED,
    )
    selection = gluex.RunSelection.runs(args.run or [50685, 50697])
    if gx.capabilities.rcdb:
        current = gx.conditions['beam_current']
        minimum_current = 10.0
        runs = gx.runs(selection).where(current > minimum_current).collect()
        print('Selected:', runs.numbers)
        print('Unknown exclusions:', runs.report.unknown_runs)
        print('Run inputs:', runs.provenance)
    if gx.capabilities.ccdb:
        table = gx.calibrations[args.table]
        print('Columns:', [(column.name, column.value_type) for column in table.columns])
        series = table.for_runs(selection).collect()
        for run, entry in series.items():
            print(run, entry.assignment_id, entry.constant_set_id)
            for name in entry.payload.columns:
                print(name, entry.payload.column(name))
        print('Missing assignments:', series.report.missing_runs)
        print('Calibration inputs:', series.provenance)


if __name__ == '__main__':
    main()
