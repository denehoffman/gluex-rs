"""
Inspect independently configured database sources without fetching payloads.

Set RCDB_CONNECTION and/or CCDB_CONNECTION to local SQLite files, or leave both
unset to explore the database-independent reference information.
"""

import gluex

gx = gluex.connect()
print(gx)
print(gx.capabilities)
print('Fall 2018 starts at run', gluex.RunPeriod('f18').min_run)

if gx.capabilities.rcdb:
    print('RCDB:', gx.sources.rcdb.connection_path)
if gx.capabilities.ccdb:
    print('CCDB:', gx.sources.ccdb.connection_path)
    print('Default calibration time:', gx.sources.ccdb.opened_at)
    print('Calibration root:', gx.sources.ccdb.root().full_path())
