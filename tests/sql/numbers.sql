DROP TABLE IF EXISTS numeric_table;

CREATE TABLE numeric_table (
    a_double double precision,
    a_real real,
    a_smallint smallint,
    a_integer integer,
    a_bigint bigint,
    a_decimal_22_9 decimal(22, 9),
    a_decimal_17_3 decimal(17, 3),
    a_smallserial smallserial,
    a_serial serial,
    a_bigserial bigserial
);

INSERT INTO
    numeric_table
VALUES
    (1.23, 4.56, 0, 0, 0, '1.2345', '1.23'),
    (7.89, 10.1112, 1, 2, 3, '12345.123456789', '123424.1345');
