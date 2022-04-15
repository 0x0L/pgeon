DROP TABLE IF EXISTS numeric_table;

CREATE TABLE numeric_table (
    a_double double precision,
    b_real real,
    c_smallint smallint,
    d_integer integer,
    e_bigint bigint,
    f_decimal_22_9 decimal(22, 9),
    g_smallserial smallserial,
    h_serial serial,
    i_bigserial bigserial
);

INSERT INTO
    numeric_table
VALUES
    (1.23, 4.56, 0, 0, 0, '1.2345'),
    (7.89, 10.1112, 1, 2, 3, '12345.123456789');