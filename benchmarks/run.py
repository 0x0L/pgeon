import asyncio
import os
import time
from io import BytesIO
from pathlib import Path

import asyncpg
import pandas as pd
import seaborn as sns

import pgeon

_PATH = Path(__file__).parent

def _bench(fn, n=1):
    elapsed = []
    for _ in range(n):
        start = time.time()
        fn()
        elapsed.append(time.time() - start)
    return elapsed


def _abench(fn, n=1):
    async def wrap():
        elapsed = []
        for _ in range(n):
            start = time.time()
            await fn()
            elapsed.append(time.time() - start)
        return elapsed
    return asyncio.run(wrap())


def asyncpg_fetch(db, query):
    async def wrap():
        conn = await asyncpg.connect(dsn=db)
        await conn.fetch(query)
    return wrap


def asyncpg_copy(db, query, format):
    async def wrap():
        conn = await asyncpg.connect(dsn=db)
        buf = BytesIO()
        await conn.copy_from_query(query, output=buf, format=format)
        buf.getvalue()
    return wrap


def pgeon_copy(db, query):
    return lambda: pgeon.copy_query(db, query)


def bench_minute_bars(db, n=1):
    print('Running minute_bars benchmark...')

    query = "select * from minute_bars"
    df = {
        'asyncpg_fetch': _abench(asyncpg_fetch(db, query), n=n),
        'asyncpg_copy_csv': _abench(asyncpg_copy(db, query, 'csv'), n=n),
        'asyncpg_copy_binary': _abench(asyncpg_copy(db, query, 'binary'), n=n),
        'python_pgeon_copy': _bench(pgeon_copy(db, query), n=n),
    }

    df = pd.DataFrame(df)
    df.to_csv(_PATH / 'minute_bars.csv', index=False)

    ax = sns.kdeplot(data=df, fill=True)
    ax.get_legend().set_frame_on(False)
    ax.figure.set_size_inches(12, 3)
    ax.xaxis.set_label_text("seconds")
    ax.yaxis.set_visible(False)
    sns.despine(left=True)

    ax.figure.tight_layout()
    ax.figure.savefig(_PATH / 'minute_bars.svg')  #, transparent=True)


if __name__ == "__main__":
    db = os.environ["PGEON_TEST_DB"]
    bench_minute_bars(db, n=100)
