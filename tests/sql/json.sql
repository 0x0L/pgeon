DROP TABLE IF EXISTS json_table;

CREATE TABLE json_table (a_json json);

INSERT INTO
    json_table
VALUES
    ('{"a": 1}' :: json),
    ('{"b": 1}' :: json),
    ('{"a": 2}' :: json);