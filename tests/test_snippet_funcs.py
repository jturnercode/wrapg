import os
from psycopg import connect
from wrapg import snippet

# Note: run test using -m flag
# pipenv run python -m pytest


conn_import: dict = {
    "user": os.environ.get("PG_USER"),
    "password": os.environ.get("PG_PASSWORD"),
    "host": os.environ.get("PG_HOST"),
    "dbname": os.environ.get("PG_DBNAME"),
    "port": os.environ.get("PG_PORT"),
}



def test_create_index_snip():

    with connect(**conn_import) as conn:

        snipp = snippet.create_unique_index(table="mytable", keys=["name", "Date(ts)"])

        assert (
            snipp.as_string(conn)
            == 'CREATE UNIQUE INDEX "mytable_name_Date(ts)_uix" ON "mytable" ("name", DATE("ts"));'
        )

        conn.close()


def test_upsert_snip():

    # Connect to an existing database
    with connect(**conn_import) as conn:

        # tests
        snipp = snippet.upsert_snip(
            table="mytable",
            columns=(("name", "age", "location")),
            keys=["name", "Date(ts)"],
        )

        compare = 'INSERT INTO "mytable" ("name", "age", "location") VALUES (%(name)s, %(age)s, %(location)s) ON CONFLICT ("name", DATE("ts")) DO UPDATE SET "name"=EXCLUDED."name", "age"=EXCLUDED."age", "location"=EXCLUDED."location";'
        assert snipp.as_string(conn) == compare

        conn.close()


def test_update_snip():

    # Connect to an existing database
    with connect(**conn_import) as conn:

        # tests
        snipp = snippet.update_snip(
            table="mytable",
            columns=(("name", "age", "location")),
            keys=["name", "Date(ts)"],
        )

        compare = 'UPDATE "mytable" SET "name"=%(name)s, "age"=%(age)s, "location"=%(location)s WHERE "name"=%(name)s, DATE("ts")=%(ts)s'
        assert snipp.as_string(conn) == compare

        conn.close()