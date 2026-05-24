CREATE TABLE runs (
    number INTEGER NOT NULL PRIMARY KEY,
    started datetime,
    finished datetime
);
CREATE TABLE condition_types (
    id INTEGER NOT NULL PRIMARY KEY,
    name TEXT NOT NULL,
    value_type TEXT NOT NULL,
    created datetime,
    description TEXT
);
CREATE TABLE conditions (
    id INTEGER NOT NULL PRIMARY KEY,
    text_value TEXT,
    int_value INTEGER NOT NULL,
    float_value REAL NOT NULL,
    bool_value INTEGER NOT NULL,
    run_number INTEGER,
    condition_type_id INTEGER,
    created datetime,
    time_value datetime
);
CREATE INDEX conditions_run_number_index ON conditions (run_number);
CREATE TABLE schema_versions (
    version INTEGER NOT NULL PRIMARY KEY
);

INSERT INTO schema_versions (version) VALUES (2);

INSERT INTO condition_types (id, name, value_type) VALUES
    (2, 'event_count', 'int'),
    (3, 'run_type', 'string'),
    (11, 'solenoid_current', 'float'),
    (13, 'status', 'int'),
    (17, 'beam_current', 'float'),
    (19, 'collimator_diameter', 'string'),
    (21, 'daq_run', 'string'),
    (28, 'is_valid_run_end', 'bool'),
    (31, 'polarization_direction', 'string'),
    (33, 'polarimeter_converter', 'string'),
    (57, 'run_start_time', 'time');

INSERT INTO runs (number) VALUES
    (2), (3), (4), (5), (1100), (10204), (50685), (50697);

INSERT INTO conditions (id, text_value, int_value, float_value, bool_value, run_number, condition_type_id, time_value) VALUES
    (1, '', 2, 0, 0, 2, 2, NULL),
    (2, '', 1686, 0, 0, 3, 2, NULL),
    (3, '', 5000, 0, 0, 4, 2, NULL),
    (4, '', 6000, 0, 0, 5, 2, NULL),
    (5, '', 0, 0, 0, 2, 28, NULL),
    (6, '', 0, 0, 1, 4, 28, NULL),
    (7, '', 0, 0, 0, 2, 57, '2015-12-08 15:47:20'),

    (10, '', 100, 0, 0, 1100, 2, NULL),
    (11, 'hd_all.tsg', 0, 0, 0, 1100, 3, NULL),
    (12, '', 0, 5.0, 0, 1100, 17, NULL),
    (13, '5.0mm hole', 0, 0, 0, 1100, 19, NULL),

    (20, '', 1000000, 0, 0, 10204, 2, NULL),
    (21, 'hd_all.tsg', 0, 0, 0, 10204, 3, NULL),
    (22, '', 0, 5.0, 0, 10204, 17, NULL),
    (23, '', 0, 1300.0, 0, 10204, 11, NULL),
    (24, '5.0mm hole', 0, 0, 0, 10204, 19, NULL),

    -- These values are sampled from the local full RCDB snapshot.
    (30, '', 124346759, 0, 0, 50685, 2, NULL),
    (31, '', 0, 148.582, 0, 50685, 17, NULL),
    (32, '', 0, 1352.73, 0, 50685, 11, NULL),
    (33, '', 1, 0, 0, 50685, 13, NULL),
    (34, '5.0mm hole', 0, 0, 0, 50685, 19, NULL),
    (35, 'PHYSICS', 0, 0, 0, 50685, 21, NULL),
    (36, 'N/A', 0, 0, 0, 50685, 31, NULL),
    (37, 'Be 75um', 0, 0, 0, 50685, 33, NULL),

    (40, '', 352576987, 0, 0, 50697, 2, NULL),
    (41, '', 0, 362.949, 0, 50697, 17, NULL),
    (42, '', 0, 1352.67, 0, 50697, 11, NULL),
    (43, '', 1, 0, 0, 50697, 13, NULL),
    (44, '5.0mm hole', 0, 0, 0, 50697, 19, NULL),
    (45, 'PHYSICS', 0, 0, 0, 50697, 21, NULL),
    (46, 'PARA', 0, 0, 0, 50697, 31, NULL),
    (47, 'Be 75um', 0, 0, 0, 50697, 33, NULL);
