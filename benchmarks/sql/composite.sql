DROP TABLE IF EXISTS on_hand;

DROP TYPE IF EXISTS inventory_item CASCADE;

CREATE TYPE inventory_item AS (
    name text,
    supplier_id integer,
    price numeric
);

CREATE TABLE on_hand (item inventory_item, count integer);

INSERT INTO
    on_hand
VALUES
    (ROW('fuzzy dice', 42, 1.99), 1000);


-- SELECT * FROM (VALUES (ROW('a', 1, 0.99)::inventory_item)) as foo;
