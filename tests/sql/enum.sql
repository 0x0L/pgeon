DROP TABLE IF EXISTS enum_table;

DROP TYPE IF EXISTS mood CASCADE;

CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy');

CREATE TABLE enum_table (a_mood mood);

INSERT INTO
    enum_table
VALUES
    ('sad'),
    ('happy');
