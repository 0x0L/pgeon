import asyncio

import asyncpg
from jinja2 import Template

DB = "postgresql://localhost/mytests"


_CREATE_TPL = Template(
    """
DROP TABLE IF EXISTS {{ name }};

CREATE TABLE {{ name }}
(
    {% for t in types[:-1] %}{{ t[0] }} {{ t[1] }},{% endfor %}
    {{ types[-1][0] }} {{ types[-1][1] }}
);

INSERT INTO {{ name }} VALUES
    {% for v in values[:-1] %}{{ v }},{% endfor %}
    {{ values[-1] }}
;
"""
)


async def numerical_types(conn):
    query = _CREATE_TPL.render(
        name="numeric",
        types=[
            ("a_double", "double precision"),
            ("b_real", "real"),
            ("c_smallint", "smallint"),
            ("d_integer", "integer"),
            ("e_bigint", "bigint"),
            ("f_decimal_22_9", "decimal(22, 9)"),
            ("g_smallserial", "smallserial"),
            ("h_serial", "serial"),
            ("i_bigserial", "bigserial"),
        ],
        values=[
            (1.23, 4.56, 0, 0, 0, "1.2345"),
            (7.89, 10.1112, 1, 2, 3, "12345.123456789"),
        ],
    )
    # print(query)
    await conn.execute(query)


async def main():
    conn = await asyncpg.connect(DB)
    await numerical_types(conn)
    await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
