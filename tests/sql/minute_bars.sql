DROP TABLE IF EXISTS minute_bars;

CREATE TABLE minute_bars (
    timestamp timestamp,
    symbol integer,
    open real,
    high real,
    low real,
    close real,
    volume integer
);

INSERT INTO
    minute_bars
SELECT
    ts,
    symbol,
    random(),
    random(),
    random(),
    random(),
    floor(random() * 10000)
FROM
    generate_series(
        '2020-01-01' :: timestamp,
        '2020-02-01' :: timestamp,
        '1 minute' :: interval
    ) AS ts,
    generate_series(0, 100) AS symbol;
