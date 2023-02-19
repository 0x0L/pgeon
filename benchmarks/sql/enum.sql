DROP TABLE IF EXISTS person;

DROP TYPE IF EXISTS mood CASCADE;

CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy');

CREATE TABLE person (name text, current_mood mood);

INSERT INTO
    person
VALUES
    ('Moe', 'happy'),
    ('Larry', 'sad'),
    ('Curly', 'ok');

SELECT
    *
FROM
    (
        VALUES
            ('Moe', 'happy'),
            ('Larry', 'sad'),
            ('Curly', 'ok')
    ) AS foo;
