DROP TABLE IF EXISTS json_table;

CREATE TABLE json_table (a_json json, a_jsonb jsonb);

INSERT INTO
    json_table
VALUES
    ('{"a": 1}' :: json, '{"a": 1}' :: jsonb),
    ('{"b": 1}' :: json, '{"b": 1}' :: jsonb),
    ('{"a": 2}' :: json, '{"a": 2}' :: jsonb);
