import asyncio
import time
from io import BytesIO

import asyncpg
import pgeon
import psycopg

import pandas as pd
import pyarrow.csv as csv
import seaborn as sns


def _df_from_buffer(buffer):
    buffer.seek(0)
    read_options = csv.ReadOptions(autogenerate_column_names=True)
    df = csv.read_csv(buffer, read_options=read_options).to_pandas()
    return df


def asyncpg_fetch(db, query):
    async def fn():
        conn = await asyncpg.connect(dsn=db)
        return pd.DataFrame(await conn.fetch(query))

    return fn


def asyncpg_copy_csv(db, query):
    async def fn():
        with BytesIO() as buf:
            conn = await asyncpg.connect(dsn=db)
            await conn.copy_from_query(query, output=buf, format="csv")
            return _df_from_buffer(buf)

    return fn


def psycopg_fetchall(db, query):
    def fn():
        with psycopg.connect(db) as conn:
            with conn.cursor(binary=True) as cur:
                cur.execute(query)
                return pd.DataFrame(cur.fetchall())

    return fn


def psycopg_copy_csv(db, query):
    def fn():
        with BytesIO() as buf, psycopg.connect(db) as conn:
            with conn.cursor(binary=True) as cur:
                with cur.copy(
                    f"COPY ({query}) TO STDOUT (FORMAT csv)",
                ) as copy:
                    for data in copy:
                        buf.write(data)
            return _df_from_buffer(buf)

    return fn


def pgeon_copy(db, query):
    def fn():
        return pgeon.copy_query(db, query).to_pandas()

    return fn


def benchmark(fn, n=1):
    elapsed = []
    for _ in range(n):
        start = time.time()
        _ = fn()
        elapsed.append(time.time() - start)
    return elapsed


def async_benchmark(fn, n=1):
    async def wrap():
        elapsed = []
        for _ in range(n):
            start = time.time()
            _ = await fn()
            elapsed.append(time.time() - start)
        return elapsed

    return asyncio.run(wrap())


def bench_minute_bars(db, n=1):
    print("Running minute_bars benchmark...")

    query = "select * from minute_bars"
    df = {
        "asyncpg_fetch": async_benchmark(asyncpg_fetch(db, query), n=n),
        "asyncpg_copy_csv": async_benchmark(asyncpg_copy_csv(db, query), n=n),
        "psycopg_fetchall": benchmark(psycopg_fetchall(db, query), n=n),
        "psycopg_copy_csv": benchmark(psycopg_copy_csv(db, query), n=n),
        "pgeon_copy": benchmark(pgeon_copy(db, query), n=n),
    }

    df = pd.DataFrame(df)
    df.to_csv("minute_bars.csv", index=False)

    ax = sns.kdeplot(data=df, fill=True)
    ax.get_legend().set_frame_on(False)
    ax.figure.set_size_inches(12, 3)
    ax.xaxis.set_label_text("seconds")
    ax.yaxis.set_visible(False)
    sns.despine(left=True)

    ax.figure.tight_layout()
    ax.figure.savefig("minute_bars.svg")  # , transparent=True)


if __name__ == "__main__":
    import os
    db = os.environ.get(
        "PGEON_TEST_DB", "postgresql://localhost:5432/postgres"
    )
    bench_minute_bars(db, n=100)
