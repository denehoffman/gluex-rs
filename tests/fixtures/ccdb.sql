CREATE TABLE directories (
    id INTEGER NOT NULL PRIMARY KEY,
    created timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
    modified timestamp NOT NULL DEFAULT '2007-01-01 00:00:00',
    name TEXT NOT NULL DEFAULT '',
    parentId INTEGER NOT NULL DEFAULT 0,
    authorId INTEGER NOT NULL DEFAULT 1,
    comment TEXT,
    isDeprecated INTEGER NOT NULL DEFAULT 0,
    deprecatedByUserId INTEGER,
    isLocked INTEGER NOT NULL DEFAULT 0,
    lockedByUserId INTEGER
);
CREATE TABLE typeTables (
    id INTEGER NOT NULL PRIMARY KEY,
    created timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
    modified timestamp NOT NULL DEFAULT '2007-01-01 00:00:00',
    directoryId INTEGER NOT NULL,
    name TEXT NOT NULL,
    nRows INTEGER NOT NULL DEFAULT 1,
    nColumns INTEGER NOT NULL,
    nAssignments INTEGER NOT NULL DEFAULT 0,
    authorId INTEGER NOT NULL DEFAULT 1,
    comment TEXT,
    isDeprecated INTEGER NOT NULL DEFAULT 0,
    deprecatedByUserId INTEGER,
    isLocked INTEGER NOT NULL DEFAULT 0,
    lockedByUserId INTEGER,
    lockTime timestamp
);
CREATE TABLE columns (
    id INTEGER NOT NULL PRIMARY KEY,
    created timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
    modified timestamp NOT NULL DEFAULT '2007-01-01 00:00:00',
    name TEXT NOT NULL,
    typeId INTEGER NOT NULL,
    columnType TEXT,
    "order" INTEGER NOT NULL,
    comment TEXT
);
CREATE TABLE variations (
    id INTEGER NOT NULL PRIMARY KEY,
    created timestamp NOT NULL DEFAULT '2007-01-01 00:00:00',
    modified timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
    name TEXT NOT NULL DEFAULT 'default',
    description TEXT,
    authorId INTEGER NOT NULL DEFAULT 1,
    comment TEXT,
    parentId INTEGER NOT NULL DEFAULT 0,
    isLocked INTEGER NOT NULL DEFAULT 0,
    lockTime timestamp,
    lockedByUserId INTEGER,
    goBackBehavior INTEGER NOT NULL DEFAULT 0,
    goBackTime timestamp,
    isDeprecated INTEGER NOT NULL DEFAULT 0,
    deprecatedByUserId INTEGER
);
CREATE TABLE constantSets (
    id INTEGER NOT NULL PRIMARY KEY,
    created timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
    modified timestamp NOT NULL DEFAULT '2007-01-01 00:00:00',
    vault TEXT NOT NULL,
    constantTypeId INTEGER NOT NULL
);
CREATE TABLE runRanges (
    id INTEGER NOT NULL PRIMARY KEY,
    created timestamp NOT NULL DEFAULT '2007-01-01 00:00:00',
    modified timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
    name TEXT DEFAULT '',
    runMin INTEGER NOT NULL,
    runMax INTEGER NOT NULL,
    comment TEXT
);
CREATE TABLE assignments (
    id INTEGER NOT NULL PRIMARY KEY,
    created timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
    modified timestamp NOT NULL DEFAULT '2007-01-01 00:00:00',
    variationId INTEGER NOT NULL,
    runRangeId INTEGER,
    eventRangeId INTEGER,
    constantSetId INTEGER NOT NULL,
    authorId INTEGER NOT NULL DEFAULT 1,
    comment TEXT
);

INSERT INTO directories (id, name, parentId) VALUES
    (1, 'test', 0),
    (20, 'demo', 1),
    (29, 'PHOTON_BEAM', 0),
    (30, 'microscope', 29),
    (32, 'pair_spectrometer', 29),
    (34, 'hodoscope', 29),
    (35, 'TARGET', 0),
    (50, 'lumi', 32),
    (51, 'tagh', 50),
    (52, 'tagm', 50);

INSERT INTO typeTables (id, directoryId, name, nRows, nColumns, nAssignments) VALUES
    (81, 20, 'mytable', 2, 3, 2),
    (217, 29, 'endpoint_energy', 1, 1, 1),
    (231, 34, 'scaled_energy_range', 1, 3, 1),
    (232, 30, 'scaled_energy_range', 1, 3, 1),
    (576, 50, 'PS_accept', 1, 3, 1),
    (592, 52, 'tagged', 1, 3, 1),
    (594, 51, 'tagged', 1, 3, 1),
    (596, 50, 'trig_live', 4, 2, 1),
    (662, 34, 'endpoint_calib', 1, 1, 0),
    (706, 35, 'density', 1, 2, 1);

INSERT INTO columns (id, name, typeId, columnType, "order") VALUES
    (641, 'x', 81, 'double', 0),
    (642, 'y', 81, 'double', 1),
    (643, 'z', 81, 'double', 2),
    (1001, 'PHOTON_BEAM_ENDPOINT_ENERGY', 217, 'double', 0),
    (1002, 'counter', 231, 'double', 0),
    (1003, 'xlow', 231, 'double', 1),
    (1004, 'xhigh', 231, 'double', 2),
    (1005, 'column', 232, 'double', 0),
    (1006, 'xlow', 232, 'double', 1),
    (1007, 'xhigh', 232, 'double', 2),
    (1008, 'Norm', 576, 'double', 0),
    (1009, 'Emin', 576, 'double', 1),
    (1010, 'Emax', 576, 'double', 2),
    (1011, 'column', 592, 'double', 0),
    (1012, 'flux', 592, 'double', 1),
    (1013, 'err_flux', 592, 'double', 2),
    (1014, 'counter', 594, 'double', 0),
    (1015, 'flux', 594, 'double', 1),
    (1016, 'err_flux', 594, 'double', 2),
    (1017, 'trigbit', 596, 'double', 0),
    (1018, 'livetime', 596, 'double', 1),
    (1019, 'TAGGER_CALIB_ENERGY', 662, 'double', 0),
    (1020, 'density', 706, 'double', 0),
    (1021, 'densityErr', 706, 'double', 1);

INSERT INTO variations (id, name, parentId) VALUES
    (1, 'default', 0),
    (2, 'mc', 1);

INSERT INTO runRanges (id, name, runMin, runMax) VALUES
    (1, 'all runs', 0, 2147483647),
    (2, '2018 fixture runs', 50685, 50697);

INSERT INTO constantSets (id, vault, constantTypeId) VALUES
    (76, '0|1|2|3|4|5', 81),
    (230302, '1|2|3|4|5|6', 81),
    (10001, '11.6300025', 217),
    (10002, '124|0.770565|0.772153', 231),
    (10003, '1|0.76508430013|0.765866632806', 232),
    (10004, '7.98871e-01|3.12448|6.32346', 576),
    (10005, '1|1905.67|46.6178', 592),
    (10006, '124|25885.5|177.084', 594),
    (10007, '0|0.96248|1|0|2|0|3|0.9682', 596),
    (10008, '70.92|0.35', 706);

INSERT INTO assignments (id, created, variationId, runRangeId, constantSetId) VALUES
    (76, '2013-02-22 13:40:35', 1, 1, 76),
    (230266, '2020-01-15 13:08:18', 1, 1, 230302),
    -- Lumi values are sampled from the local full CCDB snapshot. Their fixture
    -- effective date is normalized so REST-version tests remain deterministic.
    (10001, '2019-01-01 00:00:00', 1, 2, 10001),
    (10002, '2019-01-01 00:00:00', 1, 2, 10002),
    (10003, '2019-01-01 00:00:00', 1, 2, 10003),
    (10004, '2019-01-01 00:00:00', 1, 2, 10004),
    (10005, '2019-01-01 00:00:00', 1, 2, 10005),
    (10006, '2019-01-01 00:00:00', 1, 2, 10006),
    (10007, '2019-01-01 00:00:00', 1, 2, 10007),
    (10008, '2019-01-01 00:00:00', 1, 2, 10008);
