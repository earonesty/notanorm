import os

import pytest

from notanorm import SqliteDb
from notanorm.jsondb import JsonDb
from dotenv import load_dotenv

load_dotenv()

PYTEST_REG = False


def _safe_db_suffix(s: str) -> str:
    # keep it simple: only [a-zA-Z0-9_]
    return "".join(ch if (ch.isalnum() or ch == "_") else "_" for ch in (s or ""))


def _test_db_name(base: str = "test_db") -> str:
    """Return a unique test db name per xdist worker if available."""
    wid = os.getenv("PYTEST_XDIST_WORKER", "")
    wid = _safe_db_suffix(wid)
    return f"{base}_{wid}" if wid else base


@pytest.fixture
def db_sqlite():
    db = SqliteDb(":memory:")
    yield db
    db.close()


@pytest.fixture
def db_jsondb():
    db = JsonDb(":memory:")
    yield db
    db.close()


@pytest.fixture
def db_jsondb_notmem(tmp_path):
    db = JsonDb(str(tmp_path / "db"))
    yield db
    db.close()


@pytest.fixture
def db_sqlite_noup():
    class SqliteDbNoUp(SqliteDb):
        @property
        def _upsert_sql(self, **_):
            raise AttributeError

    db = SqliteDbNoUp(":memory:")

    assert not hasattr(db, "_upsert_sql")

    yield db

    db.close()


@pytest.fixture
def db_mysql_noup():
    from notanorm import MySqlDb

    class MySqlDbNoUp(MySqlDb):
        @property
        def _upsert_sql(self):
            raise AttributeError

    db = get_mysql_db(MySqlDbNoUp)

    assert not hasattr(db, "_upsert_sql")

    yield db

    db.close()


@pytest.fixture
def db_jsondb_noup():
    class JsonDbNoUp(JsonDb):
        @property
        def _upsert_sql(self):
            raise AttributeError

    db = JsonDbNoUp(":memory:")

    assert not hasattr(db, "_upsert_sql")

    yield db

    db.close()


@pytest.fixture
def db_sqlite_notmem(tmp_path):
    db = SqliteDb(str(tmp_path / "db"))
    yield db
    db.close()


def get_mysql_db(typ):
    dbname = _test_db_name("test_db")
    db = typ(read_default_file=os.path.expanduser("~/.my.cnf"))
    db.query(f"DROP DATABASE IF EXISTS `{dbname}`")
    db.query(f"CREATE DATABASE `{dbname}`")
    db.query(f"USE `{dbname}`")

    inst = typ(read_default_file=os.path.expanduser("~/.my.cnf"), db=dbname)
    inst._notanorm_test_dbname = dbname  # used by cleanup
    return inst


def cleanup_mysql_db(db):
    db._DbBase__closed = False
    db.query("SET SESSION TRANSACTION READ WRITE;")
    dbname = getattr(db, "_notanorm_test_dbname", "test_db")
    db.query(f"DROP DATABASE `{dbname}`")
    db.close()


@pytest.fixture
def db_mysql():
    from notanorm import MySqlDb

    db = get_mysql_db(MySqlDb)
    yield db
    cleanup_mysql_db(db)


@pytest.fixture
def db_mysql_notmem(db_mysql):
    yield db_mysql


def get_postgres_db(typ):
    import os

    dbname = _test_db_name("test_db")

    # Use environment variables or defaults for postgres connection
    # Similar to mysql's ~/.my.cnf approach
    db = typ(
        host=os.getenv("PGHOST", "localhost"),
        port=int(os.getenv("PGPORT", "5432")),
        user=os.getenv("PGUSER", "postgres"),
        password=os.getenv("PGPASSWORD", ""),
    )
    # Ensure no leaked connections prevent dropping between tests/runs
    try:
        # Postgres 13+: FORCE terminates connections as part of DROP DATABASE
        db.query(f'DROP DATABASE "{dbname}" WITH (FORCE)')
    except Exception:
        db.query(
            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = %s AND pid <> pg_backend_pid()",
            dbname,
        )
        db.query(f'DROP DATABASE IF EXISTS "{dbname}"')
    db.query(f'CREATE DATABASE "{dbname}"')
    db.close()

    inst = typ(
        host=os.getenv("PGHOST", "localhost"),
        port=int(os.getenv("PGPORT", "5432")),
        user=os.getenv("PGUSER", "postgres"),
        password=os.getenv("PGPASSWORD", ""),
        dbname=dbname,
    )
    inst._notanorm_test_dbname = dbname  # used by cleanup
    return inst


def cleanup_postgres_db(db):
    db._DbBase__closed = False
    # Close connection to test_db
    dbname = getattr(db, "_notanorm_test_dbname", "test_db")
    db.close()
    # Connect to postgres database to drop test_db
    import os
    from notanorm import PostgresDb

    cleanup_db = PostgresDb(
        host=os.getenv("PGHOST", "localhost"),
        port=int(os.getenv("PGPORT", "5432")),
        user=os.getenv("PGUSER", "postgres"),
        password=os.getenv("PGPASSWORD", ""),
    )
    try:
        cleanup_db.query(f'DROP DATABASE "{dbname}" WITH (FORCE)')
    except Exception:
        cleanup_db.query(
            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = %s AND pid <> pg_backend_pid()",
            dbname,
        )
        cleanup_db.query(f'DROP DATABASE IF EXISTS "{dbname}"')
    cleanup_db.close()


@pytest.fixture
def db_postgres():
    from notanorm import PostgresDb

    db = get_postgres_db(PostgresDb)
    yield db
    cleanup_postgres_db(db)


@pytest.fixture
def db_postgres_notmem(db_postgres):
    yield db_postgres


@pytest.fixture
def db_postgres_noup():
    from notanorm import PostgresDb

    class PostgresDbNoUp(PostgresDb):
        @property
        def _upsert_sql(self):
            raise AttributeError

    db = get_postgres_db(PostgresDbNoUp)

    assert not hasattr(db, "_upsert_sql")

    yield db

    cleanup_postgres_db(db)


@pytest.fixture(name="db")
def db_fixture(request, db_name):
    yield request.getfixturevalue("db_" + db_name)


@pytest.fixture(name="db_sqlup", params=["", "_noup"])
def db_sqlup_fixture(request, db_name):
    yield request.getfixturevalue("db_" + db_name + request.param)


@pytest.fixture(name="db_notmem")
def db_notmem_fixture(request, db_name):
    yield request.getfixturevalue("db_" + db_name + "_notmem")


def pytest_generate_tests(metafunc):
    """Converts user-argument --db to fixture parameters."""

    global PYTEST_REG  # pylint: disable=global-statement
    if not PYTEST_REG:
        if any(db in metafunc.fixturenames for db in ("db", "db_notmem", "db_sqlup")):
            db_names = metafunc.config.getoption("db", [])
            db_names = db_names or ["sqlite"]
            for mark in metafunc.definition.own_markers:
                if mark.name == "db":
                    db_names = set(mark.args).intersection(set(db_names))
                    break
            db_names = sorted(db_names)  # xdist compat
            metafunc.parametrize("db_name", db_names, scope="function")
