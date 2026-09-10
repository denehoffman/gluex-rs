"""
Discover conditions, resolve recorded runs and inspect raw rows.

Run with RCDB_CONNECTION set to an unchanged local SQLite file:
    python examples/database_reads.py
"""

import gluex


def main() -> None:
    gx = gluex.open(ccdb=gluex.DISABLED)
    catalog = gx.runs.conditions
    print(f'{len(catalog)} condition definitions')
    for name, definition in catalog.items()[:5]:
        print(name, definition.value_type, definition.description)

    query = gx.runs.select(gluex.RunPeriod.RP2018_08)
    print(query)  # Does not retrieve runs.
    result = query.collect()
    print('Recorded runs:', result.numbers)
    print('Resolution inputs:', result.provenance)

    rows = gx.sources.rcdb.raw(
        'SELECT number FROM runs WHERE number BETWEEN ? AND ? ORDER BY number',
        parameters=[50000, 59999],
    )
    print('Columns:', rows.columns)
    for row in rows.rows:
        print(row.values)


if __name__ == '__main__':
    main()
