import re
import logging
from collections import defaultdict
from typing import Tuple, Any, Callable, Dict
from functools import partial
import psycopg2
import psycopg2.extras
from psycopg2 import sql
from .base import DbBase, parse_bool
from .model import (
    DbType,
    DbModel,
    DbTable,
    DbCol,
    DbIndex,
    DbIndexField,
    DbColCustomInfo,
)
from . import errors as err

log = logging.getLogger(__name__)


class PostgresDb(DbBase):
    uri_name = "postgres"

    placeholder = "%s"
    default_values = " DEFAULT VALUES"
    max_index_name = 63  # PostgreSQL has a 63 character limit for index names

    # Map DbCol.size byte widths to Postgres integer types (Postgres has no 1-byte int)
    _int_map = {1: "smallint", 2: "smallint", 4: "integer", 8: "bigint"}

    def limit_query(self, limit):
        """PostgreSQL uses LIMIT <rows> OFFSET <offset>, not LIMIT <offset>, <rows>."""
        try:
            offset, rows = limit
            return f"limit {rows} offset {offset}"
        except TypeError:
            return f"limit {limit}"

    def _begin(self, conn):
        # PostgreSQL needs to disable autocommit for transactions
        if conn.autocommit:
            conn.autocommit = False
        conn.cursor().execute("BEGIN")

    @classmethod
    def uri_adjust(cls, args, kws):
        # Convert database kwarg to dbname (PostgreSQL uses dbname)
        if "database" in kws and "dbname" not in kws:
            kws["dbname"] = kws.pop("database")

        # adjust to appropriate types
        typ: Callable[[Any], Any]
        for nam, typ in [
            ("port", int),
            ("use_unicode", bool),
            ("autocommit", bool),
            ("pool_size", int),
        ]:
            if nam in kws:
                if typ is bool:
                    typ = partial(parse_bool, nam)
                kws[nam] = typ(kws[nam])

        if args:
            kws["host"] = args[0]
            args.clear()

    def _upsert_sql(self, table, inssql, insvals, setsql, setvals):
        fields = self.primary_fields(table)
        f0 = next(iter(fields))
        f0_quoted = self.quote_key(f0)

        # Check if table has a single auto-increment primary key to add RETURNING
        returning = None
        try:
            tab = self.model(no_capture=True)[table]
            autoinc_prim = [
                c.name for c in tab.columns if c.autoinc and c.name in fields
            ]
            log.debug(
                "_upsert_sql for table %s: autoinc_prim = %s, fields = %s",
                table,
                autoinc_prim,
                fields,
            )
            if len(autoinc_prim) == 1:
                returning = autoinc_prim[0]
                log.debug("_upsert_sql: adding RETURNING %s", returning)
        except Exception as e:
            log.debug("_upsert_sql: exception getting model: %s", e)
            returning = None

        if not setvals:
            sql = inssql + f" ON CONFLICT ({f0_quoted}) DO NOTHING"
        else:
            sql = inssql + f" ON CONFLICT ({f0_quoted}) DO UPDATE SET {setsql}"

        # Add RETURNING clause for auto-increment primary keys
        if returning:
            returning_quoted = self.quote_key(returning)
            sql += f" RETURNING {returning_quoted}"

        if not setvals:
            return sql, insvals
        return sql, (*insvals, *setvals)

    @staticmethod
    def translate_error(exp):
        msg = str(exp)
        # psycopg2 raises specialized error classes in psycopg2.errors
        if isinstance(exp, psycopg2.InterfaceError):
            return err.DbConnectionError(msg)
        # Only treat OperationalError as a connection error in specific cases
        if isinstance(exp, psycopg2.OperationalError):
            # Used internally by _executeone() to trigger base retry behavior
            if "Transaction aborted, retry needed" in msg:
                return err.DbConnectionError(msg)
            low = msg.lower()
            if any(
                s in low
                for s in (
                    "server closed the connection unexpectedly",
                    "terminating connection",
                    "connection not open",
                    "could not connect",
                    "connection refused",
                    "connection timed out",
                    "ssl connection has been closed",
                )
            ):
                return err.DbConnectionError(msg)
            return err.OperationalError(msg)

        if isinstance(exp, psycopg2.errors.DuplicateTable):
            return err.TableExistsError(msg)
        if isinstance(exp, psycopg2.errors.UndefinedTable):
            return err.TableNotFoundError(msg)
        if isinstance(exp, psycopg2.errors.UndefinedColumn):
            return err.NoColumnError(msg)
        # Syntax / programming errors should map to OperationalError (matches sqlite/mysql behavior)
        # Keep this AFTER specific mappings like UndefinedTable/UndefinedColumn.
        if isinstance(exp, (psycopg2.errors.SyntaxError, psycopg2.ProgrammingError)):
            return err.OperationalError(msg)
        if isinstance(
            exp,
            (
                psycopg2.errors.UniqueViolation,
                psycopg2.errors.ForeignKeyViolation,
                psycopg2.errors.NotNullViolation,
            ),
        ):
            return err.IntegrityError(msg)
        # Check for read-only errors
        if isinstance(exp, psycopg2.errors.ReadOnlySqlTransaction):
            return err.DbReadOnlyError(msg)
        return exp

    @staticmethod
    def _rewrite_sql(sql_txt: str) -> str:
        """Rewrite SQL to be PostgreSQL-compatible"""
        # Rewrite double to double precision
        sql_txt = re.sub(
            r'(?<!")\bdouble\b(?!")\b(?!\s+precision)',
            "double precision",
            sql_txt,
            flags=re.IGNORECASE,
        )
        # Normalize blob types to bytea
        sql_txt = re.sub(
            r'(?<!")\b(binary|varbinary)\s*\(\s*\d+\s*\)(?!")',
            "bytea",
            sql_txt,
            flags=re.IGNORECASE,
        )
        sql_txt = re.sub(
            r'(?<!")\b(blob|mediumblob|longblob)\s*\(\s*\d+\s*\)(?!")',
            "bytea",
            sql_txt,
            flags=re.IGNORECASE,
        )
        sql_txt = re.sub(
            r'(?<!")\b(mediumblob|longblob|blob|binary|varbinary)\b(?!")',
            "bytea",
            sql_txt,
            flags=re.IGNORECASE,
        )
        sql_txt = re.sub(
            r'(?<!")\bbytea\s*\(\s*\d+\s*\)(?!")', "bytea", sql_txt, flags=re.IGNORECASE
        )
        sql_txt = re.sub(
            r'(?<!")\b(mediumtext|longtext)\b(?!")',
            "text",
            sql_txt,
            flags=re.IGNORECASE,
        )
        # MySQL tinyint has no direct Postgres equivalent; treat it as smallint
        sql_txt = re.sub(
            r'(?<!")\btinyint\b(?!")', "smallint", sql_txt, flags=re.IGNORECASE
        )
        # Rewrite auto_increment to serial
        sql_txt = re.sub(
            r'(?<!")\bsmallint\s+auto_increment\b(?!")',
            "smallserial",
            sql_txt,
            flags=re.IGNORECASE,
        )
        sql_txt = re.sub(
            r'(?<!")\binteger\s+auto_increment\b(?!")',
            "serial",
            sql_txt,
            flags=re.IGNORECASE,
        )
        sql_txt = re.sub(
            r'(?<!")\bbigint\s+auto_increment\b(?!")',
            "bigserial",
            sql_txt,
            flags=re.IGNORECASE,
        )
        sql_txt = re.sub(
            r'(?<!")\bauto_increment\b(?!")', "", sql_txt, flags=re.IGNORECASE
        )

        # Quote mixed-case column identifiers in CREATE TABLE to preserve case
        create_table_match = re.search(
            r"\bcreate\s+table\s+[^(]*\(([^)]+)\)", sql_txt, re.IGNORECASE | re.DOTALL
        )
        if create_table_match:
            cols_def = create_table_match.group(1)
            # Split column definitions by comma, being careful of nested parens
            parts = []
            depth = 0
            start = 0
            for i, ch in enumerate(cols_def):
                if ch == "(":
                    depth += 1
                elif ch == ")":
                    depth -= 1
                elif ch == "," and depth == 0:
                    parts.append(cols_def[start:i].strip())
                    start = i + 1
            parts.append(cols_def[start:].strip())

            quoted_parts = []
            for part in parts:
                # Match column name at start: identifier followed by whitespace and type
                col_match = re.match(r"^([a-zA-Z_][a-zA-Z0-9_]*)(\s+)", part)
                if col_match:
                    col_name = col_match.group(1)
                    # Quote if mixed case (has both upper and lower) and not already quoted
                    if (
                        col_name != col_name.lower()
                        and col_name != col_name.upper()
                        and not col_name.startswith('"')
                    ):
                        quoted_name = '"' + col_name.replace('"', '""') + '"'
                        part = (
                            quoted_name
                            + col_match.group(2)
                            + part[len(col_match.group(0)) :]
                        )
                quoted_parts.append(part)

            # Replace the column definitions in the original SQL
            new_cols_def = ", ".join(quoted_parts)
            sql_txt = (
                sql_txt[: create_table_match.start(1)]
                + new_cols_def
                + sql_txt[create_table_match.end(1) :]
            )

        return sql_txt

    @staticmethod
    def _has_returning_clause(sql_txt: str) -> bool:
        """Detect a RETURNING clause, ignoring quoted identifiers/strings.

        This avoids false positives like a column named "returning".
        """
        out = []
        in_s = False  # single quotes
        in_d = False  # double quotes
        i = 0
        while i < len(sql_txt):
            ch = sql_txt[i]
            if in_s:
                if ch == "'":
                    # handle escaped single quote '' inside a string
                    if i + 1 < len(sql_txt) and sql_txt[i + 1] == "'":
                        i += 2
                        continue
                    in_s = False
                i += 1
                continue
            if in_d:
                if ch == '"':
                    # handle escaped double quote "" inside an identifier
                    if i + 1 < len(sql_txt) and sql_txt[i + 1] == '"':
                        i += 2
                        continue
                    in_d = False
                i += 1
                continue

            if ch == "'":
                in_s = True
                i += 1
                continue
            if ch == '"':
                in_d = True
                i += 1
                continue

            out.append(ch)
            i += 1

        return re.search(r"\breturning\b", "".join(out), re.IGNORECASE) is not None

    def _executeone(self, cursor, sql: str, parameters: Tuple[Any, ...]):
        sql = self._rewrite_sql(sql)

        # If this is an INSERT produced by DbBase.insert(), add RETURNING for a single autoinc PK
        # so cursor.lastrowid behaves like other backends (without overriding DbBase.insert()).
        try:
            if not self._has_returning_clause(sql):
                m = re.search(
                    r'\binsert\s+into\s+(?:"([^"]+)"|([a-zA-Z_][a-zA-Z0-9_]*))',
                    sql,
                    re.IGNORECASE,
                )
                if m:
                    table_name = m.group(1) or m.group(2)
                    # Determine if the INSERT explicitly includes a column list with the autoinc PK
                    cols_in_insert = None
                    after = sql[m.end() :].lstrip()
                    if after.startswith("("):
                        end = after.find(")")
                        if end != -1:
                            cols_in_insert = [
                                c.strip().strip('"') for c in after[1:end].split(",")
                            ]

                    prim = self.primary_fields(table_name)
                    tab = self.model(no_capture=True)[table_name]
                    autoinc_prim = [
                        c.name for c in tab.columns if c.autoinc and c.name in prim
                    ]
                    if len(autoinc_prim) == 1:
                        pk = autoinc_prim[0]
                        if cols_in_insert is None or pk not in cols_in_insert:
                            sql += f" RETURNING {self.quote_key(pk)}"
                            # Mark so our cursor wrapper knows it's safe to consume one row
                            setattr(cursor, "_notanorm_returning", True)
        except Exception:
            # If anything goes wrong (missing model, etc.), just run the original INSERT
            pass

        # For INSERT statements, map lowercase column names to actual quoted column names
        # Only do this for user tables (not system tables like pg_tables, information_schema, etc.)
        insert_match = re.search(
            r'\binsert\s+into\s+(?:"([^"]+)"|([a-zA-Z_][a-zA-Z0-9_]*))\s*\(([^)]+)\)',
            sql,
            re.IGNORECASE,
        )
        if insert_match:
            table_name = insert_match.group(1) or insert_match.group(2)
            # Skip system tables and views
            if table_name not in (
                "pg_tables",
                "pg_indexes",
                "pg_attribute",
                "pg_class",
                "pg_namespace",
                "information_schema",
            ):
                cols_list = insert_match.group(3)
                try:
                    # Query pg_attribute to get actual column names
                    temp_cursor = cursor.connection.cursor()
                    temp_cursor.execute(
                        """
                        SELECT a.attname
                        FROM pg_attribute a
                        JOIN pg_class c ON c.oid = a.attrelid
                        JOIN pg_namespace n ON n.oid = c.relnamespace
                        WHERE n.nspname = 'public' AND c.relname = %s
                        AND a.attnum > 0 AND NOT a.attisdropped
                        ORDER BY a.attnum
                    """,
                        (table_name,),
                    )
                    actual_cols = [row[0] for row in temp_cursor.fetchall()]
                    temp_cursor.close()

                    if actual_cols:  # Only proceed if table exists
                        # Map lowercase names to actual names
                        col_map = {col.lower(): col for col in actual_cols}

                        # Rewrite column list to use actual quoted names
                        cols = [c.strip() for c in cols_list.split(",")]
                        quoted_cols = []
                        for col in cols:
                            col_match = re.match(
                                r'^([a-zA-Z_][a-zA-Z0-9_]*|"[^"]+")', col
                            )
                            if col_match:
                                ident = col_match.group(1)
                                if not ident.startswith('"'):
                                    # Map to actual column name if it exists
                                    actual_name = col_map.get(ident.lower())
                                    if (
                                        actual_name
                                        and actual_name != actual_name.lower()
                                    ):
                                        ident = (
                                            '"' + actual_name.replace('"', '""') + '"'
                                        )
                                quoted_cols.append(
                                    ident + col[len(col_match.group(0)) :]
                                )
                            else:
                                quoted_cols.append(col)
                        new_cols_list = ", ".join(quoted_cols)
                        sql = (
                            sql[: insert_match.start(3)]
                            + new_cols_list
                            + sql[insert_match.end(3) :]
                        )
                except Exception:
                    pass  # If we can't get column names, use original SQL

        # If this statement uses RETURNING, tell the cursor wrapper so it can populate lastrowid
        # (covers upsert RETURNING as well as injected INSERT RETURNING)
        if self._has_returning_clause(sql):
            try:
                setattr(cursor, "_notanorm_returning", True)
            except Exception:
                pass

        try:
            return cursor.execute(sql, parameters)
        except psycopg2.errors.InFailedSqlTransaction as ex:
            # Transaction was aborted due to a previous error
            # Rollback the connection and close the cursor so execute() can get a fresh one
            log.debug("postgres transaction aborted, rolling back")
            try:
                cursor.connection.rollback()
                cursor.close()
            except Exception:
                pass
            # Re-raise so execute() can retry with a fresh cursor
            raise psycopg2.errors.OperationalError(
                "Transaction aborted, retry needed"
            ) from ex
        except psycopg2.errors.UndefinedFunction as ex:
            msg = str(ex)
            if "operator does not exist: text =" in msg:
                log.debug(
                    "postgres retrying with coerced params for: %s", msg.splitlines()[0]
                )
                coerced = tuple(
                    str(p) if isinstance(p, (int, float, bool)) else p
                    for p in parameters
                )
                return cursor.execute(sql, coerced)
            raise

    def _connect(self, *args, **kws):
        conn = psycopg2.connect(*args, **kws)
        # Set autocommit to True for normal operations (DDL/DML auto-committed)
        # Transactions will temporarily disable it in _begin
        conn.autocommit = True
        return conn

    def _commit(self, conn):
        conn.commit()
        # Restore autocommit after transaction
        conn.autocommit = True

    def _rollback(self, conn):
        conn.rollback()
        # Restore autocommit after transaction
        conn.autocommit = True

    def _cursor(self, conn):
        return conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    def execute(
        self, sql: str, parameters=(), _script=False, write=True, no_capture=False
    ):
        """Override execute to return a cursor that normalizes BYTEA (memoryview to bytes) and handles RETURNING.

        Also renders psycopg2.sql objects (Composed/SQL/Identifier/...) to a string before calling DbBase.execute,
        since DbBase expects a string SQL.
        """
        if not isinstance(sql, str):
            # psycopg2.sql.Composed / SQL objects
            try:
                sql = sql.as_string(self._conn())
            except Exception:
                sql = str(sql)

        cursor = super().execute(
            sql, parameters, _script=_script, write=write, no_capture=no_capture
        )

        # Wrap cursor to normalize BYTEA (memoryview to bytes) and handle RETURNING for lastrowid
        class ByteaNormalizingCursor:
            def __init__(self, cursor):
                self._cursor = cursor
                self._lastrowid = None
                self._returning_row = None
                # If driver injected RETURNING, fetch the result immediately to populate lastrowid
                if (
                    getattr(cursor, "_notanorm_returning", False)
                    and cursor
                    and getattr(cursor, "description", None)
                ):
                    try:
                        row = cursor.fetchone()
                        if row:
                            self._returning_row = row
                            # Extract lastrowid from RETURNING result
                            if hasattr(row, "items"):
                                self._lastrowid = list(row.values())[0] if row else None
                            elif isinstance(row, (tuple, list)) and len(row) > 0:
                                self._lastrowid = row[0]
                    except Exception as e:
                        log.debug(
                            "ByteaNormalizingCursor: error fetching RETURNING: %s", e
                        )
                        pass

            def __getattr__(self, name):
                # Delegate all attribute access to the underlying cursor
                return getattr(self._cursor, name)

            @property
            def lastrowid(self):
                # Return stored lastrowid or delegate to underlying cursor
                if self._lastrowid is not None:
                    return self._lastrowid
                return getattr(self._cursor, "lastrowid", None)

            @lastrowid.setter
            def lastrowid(self, value):
                self._lastrowid = value

            def __iter__(self):
                # Make cursor iterable
                return iter(self._cursor)

            def _normalize_row(self, row):
                """Convert memoryview to bytes in row data"""
                if hasattr(row, "items"):
                    return {
                        k: (bytes(v) if isinstance(v, memoryview) else v)
                        for k, v in row.items()
                    }
                elif isinstance(row, (tuple, list)):
                    return tuple(
                        bytes(v) if isinstance(v, memoryview) else v for v in row
                    )
                return row

            def fetchall(self):
                # Handle DDL statements that have no results
                if getattr(self._cursor, "description", None) is None:
                    return []
                try:
                    rows = self._cursor.fetchall()
                    return [self._normalize_row(row) for row in rows]
                except psycopg2.ProgrammingError as e:
                    if "no results to fetch" in str(e):
                        return []
                    raise

            def fetchone(self):
                # Handle DDL statements that have no results
                if getattr(self._cursor, "description", None) is None:
                    return None
                try:
                    # If we stored the RETURNING row, return it first
                    if self._returning_row is not None:
                        row = self._returning_row
                        self._returning_row = None
                        return self._normalize_row(row)
                    row = self._cursor.fetchone()
                    return self._normalize_row(row) if row else None
                except psycopg2.ProgrammingError as e:
                    if "no results to fetch" in str(e):
                        return None
                    raise

            def fetchmany(self, size=None):
                # Handle DDL statements that have no results
                if getattr(self._cursor, "description", None) is None:
                    return []
                try:
                    rows = self._cursor.fetchmany(size)
                    return [self._normalize_row(row) for row in rows]
                except psycopg2.ProgrammingError as e:
                    if "no results to fetch" in str(e):
                        return []
                    raise

            def close(self):
                return self._cursor.close()

        return ByteaNormalizingCursor(cursor) if cursor else cursor

    @classmethod
    def quote_key(cls, key):
        # DbBase expects quote_key to return a SQL string fragment, not a sql.Identifier
        return '"' + str(key).replace('"', '""') + '"'

    def _get_primary(self, table):
        # Query to get primary key columns for a specific table
        # Use a more explicit query that ensures we only get columns from the target table
        sql_query = """
            SELECT a.attname
            FROM pg_index i
            JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
            JOIN pg_class c ON c.oid = i.indrelid
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = 'public'
            AND c.relname = %s
            AND i.indisprimary = true
            AND a.attrelid = c.oid
            ORDER BY a.attnum;
        """
        cursor = self.execute(sql_query, (table,), write=False, no_capture=True)
        prim: list[str] = []
        try:
            rows = cursor.fetchall()
            log.debug("_get_primary for table %s returned %d rows", table, len(rows))
            for row in rows:
                if hasattr(row, "attname"):
                    col_name = row.attname
                elif isinstance(row, (tuple, list)) and len(row) > 0:
                    col_name = row[0]
                elif isinstance(row, dict):
                    col_name = row.get("attname", list(row.values())[0] if row else "")
                else:
                    continue
                log.debug("_get_primary: found column %s", col_name)
                prim.append(col_name)
        finally:
            cursor.close()
        ret = tuple(prim)
        log.debug("_get_primary for table %s returning %s", table, ret)
        return ret

    _type_map = {
        DbType.TEXT: "text",
        DbType.BLOB: "bytea",
        DbType.INTEGER: "bigint",
        DbType.BOOLEAN: "boolean",
        DbType.FLOAT: "real",
        DbType.DOUBLE: "double precision",
        DbType.ANY: "",  # You can customize this mapping as needed
    }
    _type_map_inverse = {
        v: k for k, v in _type_map.items()
    }  # You can customize this mapping as needed
    # Accept common aliases returned by information_schema
    _type_map_inverse.update(
        {
            "integer": DbType.INTEGER,
            "smallint": DbType.INTEGER,
            "bigint": DbType.INTEGER,
            "character varying": DbType.TEXT,
            "character": DbType.TEXT,
            "varchar": DbType.TEXT,
            "text": DbType.TEXT,
            "bytea": DbType.BLOB,
        }
    )
    # Postgres column custom info isn't needed for round-tripping; keep it empty so
    # model equality (e.g. ddl -> model_from_ddl) doesn't diverge.
    _type_map_custom: Dict[str, DbColCustomInfo] = {}

    def create_table(
        self, name, schema: DbTable, ignore_existing=False, create_indexes: bool = True
    ):
        coldefs = []
        primary_fields: tuple[str, ...] = ()
        for idx in schema.indexes:
            if idx.primary:
                primary_fields = tuple(f.name for f in idx.fields)

        for col in schema.columns:
            if col.typ == DbType.ANY:
                raise err.SchemaError(
                    f"DbType.ANY is not a concrete PostgreSQL type: {name}.{col.name}"
                )
            coldef = sql.Identifier(col.name)
            # Autoincrement integer primary keys are represented as serial/bigserial
            if col.autoinc and col.typ == DbType.INTEGER:
                if (col.name,) != primary_fields:
                    raise err.SchemaError(
                        f"auto increment only works on primary key: {col.name}"
                    )
                # serial is 4-byte; bigserial is 8-byte
                if col.size and col.size > 4:
                    typ = sql.SQL("bigserial")
                else:
                    typ = sql.SQL("serial")
            elif (
                col.custom
                and col.typ == DbType.TEXT
                and col.custom.dialect == "postgres"
            ):
                if col.custom.info == "text":
                    typ = "text"
                elif col.custom.info == "bytea":
                    typ = "bytea"
                else:
                    assert False, "unknown custom info"
            elif col.size and col.typ == DbType.TEXT:
                # Fixed-width text should be CHAR(n), variable-width should be VARCHAR(n)
                if col.fixed:
                    typ = sql.SQL("char({})").format(sql.Literal(col.size))
                else:
                    typ = sql.SQL("varchar({})").format(sql.Literal(col.size))
            elif col.size and col.typ == DbType.BLOB:
                typ = sql.SQL("bytea")
            elif col.size and col.typ == DbType.INTEGER and col.size in self._int_map:
                typ = sql.SQL(self._int_map[col.size])
            else:
                typ = sql.SQL(self._type_map[col.typ])

            coldef += sql.SQL(" ") + typ
            if col.notnull:
                coldef += sql.SQL(" NOT NULL")
            if col.default:
                coldef += sql.SQL(" DEFAULT ") + sql.Literal(col.default)
            coldefs.append(coldef)
        keys = [sql.Identifier(k) for k in primary_fields]
        if keys:
            coldefs.append(sql.SQL("PRIMARY KEY ({})").format(sql.SQL(", ").join(keys)))

        ignore = sql.SQL("IF NOT EXISTS ") if ignore_existing else sql.SQL("")
        # No trailing ';' here: DbBase.ddl_from_model() appends it.
        create = sql.SQL("CREATE TABLE {}{} ({})").format(
            ignore, sql.Identifier(name), sql.SQL(", ").join(coldefs)
        )
        self.query(create)

        if create_indexes:
            self.create_indexes(name, schema)

    def _create_index(self, table_name, index_name, idx):
        # Primary key index is created by the table's PRIMARY KEY constraint
        if idx.primary:
            return
        unique = "UNIQUE " if idx.unique else ""

        def _idx_expr(field: DbIndexField):
            if field.prefix_len is None:
                return sql.Identifier(field.name)
            # PostgreSQL doesn't support MySQL-style prefix indexes (col(n)).
            # Use an expression index instead.
            return sql.SQL("left({}, {})").format(
                sql.Identifier(field.name),
                sql.Literal(int(field.prefix_len)),
            )

        icreate = sql.SQL("CREATE {}INDEX {} ON {} ({})").format(
            sql.SQL(unique),
            sql.Identifier(index_name),
            sql.Identifier(table_name),
            sql.SQL(", ").join(_idx_expr(f) for f in idx.fields),
        )
        self.execute(icreate)

    def model(self, no_capture=False):
        tabs = self.query(
            "SELECT tablename FROM pg_tables WHERE schemaname = 'public'",
            no_capture=no_capture,
        )
        ret = DbModel()
        for tab in tabs:
            ret[tab[0]] = self.table_model(tab[0], no_capture=no_capture)
        return ret

    def drop_index_by_name(self, table: str, index_name):
        # PostgreSQL syntax: DROP INDEX index_name  (no "ON table" clause)
        # Don't use IF EXISTS here: DbBase.drop_index_by_name raises when missing, and tests expect that.
        stmt = sql.SQL("DROP INDEX {}").format(sql.Identifier(index_name))
        self.execute(stmt)

    def table_model(self, tab, no_capture):
        # Get primary key fields first
        primary_fields = tuple(self._get_primary(tab))

        def _unquote_ident(s: str) -> str:
            s = s.strip()
            if s.startswith('"') and s.endswith('"') and len(s) >= 2:
                return s[1:-1].replace('""', '"')
            return s

        def _strip_parens(s: str) -> str:
            s = s.strip()
            while s.startswith("(") and s.endswith(")"):
                inner = s[1:-1].strip()
                if not inner:
                    break
                s = inner
            return s

        def _parse_left(part_s: str) -> tuple[str, int] | None:
            s = part_s.strip()
            # Accept left(...) and "left"(...) (quoted function name)
            m0 = re.match(r'(?is)^(?:"left"|left)\s*\(', s)
            if not m0:
                return None
            start = s.find("(")
            end = s.rfind(")")
            if start == -1 or end == -1 or end <= start:
                return None
            inner = s[start + 1 : end]
            # Split args on top-level comma
            args: list[str] = []
            abuf: list[str] = []
            depth2 = 0
            in_q2 = False
            j = 0
            while j < len(inner):
                ch2 = inner[j]
                if ch2 == '"':
                    if in_q2 and j + 1 < len(inner) and inner[j + 1] == '"':
                        abuf.append('"')
                        j += 2
                        continue
                    in_q2 = not in_q2
                    abuf.append(ch2)
                    j += 1
                    continue
                if not in_q2:
                    if ch2 == "(":
                        depth2 += 1
                    elif ch2 == ")":
                        depth2 = max(0, depth2 - 1)
                    elif ch2 == "," and depth2 == 0:
                        arg = "".join(abuf).strip()
                        if arg:
                            args.append(arg)
                        abuf = []
                        j += 1
                        continue
                abuf.append(ch2)
                j += 1
            last = "".join(abuf).strip()
            if last:
                args.append(last)
            if len(args) != 2:
                return None
            # First arg may include casts/parentheses; drop casts and unwrap parens.
            col_expr = args[0].split("::", 1)[0]
            col_expr = _strip_parens(col_expr)
            col_name = _unquote_ident(col_expr.split()[0])
            try:
                n = int(args[1].strip())
            except Exception:
                return None
            return col_name, n

        # Prefer pg_catalog introspection over parsing pg_indexes.indexdef.
        # This also works for expression indexes via pg_get_indexdef().
        res = self.query(
            """
            WITH idx AS (
              SELECT
                i.indexrelid,
                ic.relname                    AS index_name,
                i.indisprimary                AS is_primary,
                i.indisunique                 AS is_unique,
                i.indnkeyatts                 AS nkeyatts,
                i.indnatts                    AS natts
              FROM pg_index i
              JOIN pg_class ic ON ic.oid = i.indexrelid
              WHERE i.indrelid = to_regclass(%s)
            )
            SELECT
              idx.index_name,
              idx.is_primary,
              idx.is_unique,
              k.pos,
              pg_get_indexdef(idx.indexrelid, k.pos, true) AS keydef,
              (k.pos > idx.nkeyatts) AS is_included
            FROM idx
            JOIN LATERAL generate_series(1, idx.natts) AS k(pos) ON true
            ORDER BY idx.index_name, k.pos
            """,
            (self.quote_key(tab),),
            no_capture=no_capture,
        )

        idxmeta: dict[str, dict[str, Any]] = {}
        idxfields: dict[str, list[dict[str, Any]]] = defaultdict(list)

        for row in res:
            idxname = row.index_name if hasattr(row, "index_name") else row[0]
            is_primary = bool(row.is_primary if hasattr(row, "is_primary") else row[1])
            is_unique = bool(row.is_unique if hasattr(row, "is_unique") else row[2])
            keydef = row.keydef if hasattr(row, "keydef") else row[4]
            is_included = bool(
                row.is_included if hasattr(row, "is_included") else row[5]
            )

            if not idxname:
                continue
            if idxname not in idxmeta:
                idxmeta[idxname] = {"primary": is_primary, "unique": is_unique}
            if is_included:
                continue

            # Map expression indexes back to a plausible column name when possible.
            # For left(col, n) emulation we keep the column name (prefix_len is normalized away).
            left_parsed = _parse_left(keydef)
            if left_parsed:
                coln, n = left_parsed
                idxfields[idxname].append({"name": coln, "prefix_len": n})
            else:
                col = _strip_parens(str(keydef)).strip()
                # pg_get_indexdef(..., pretty=true) often returns a bare identifier or a quoted one.
                # If it's more complex, keep the raw text (best-effort).
                idxfields[idxname].append(
                    {"name": _unquote_ident(col), "prefix_len": None}
                )

        indexes = []
        for name, fds in idxfields.items():
            # Postgres doesn't preserve "prefix length" semantics (we emulate with expression
            # indexes), and tests expect prefix_len=None for postgres.
            fds_norm = [{"name": fd["name"], "prefix_len": None} for fd in fds]
            primary = bool(idxmeta.get(name, {}).get("primary"))
            # Primary key in notanorm is represented as primary=True, unique=False
            unique = False if primary else bool(idxmeta.get(name, {}).get("unique"))
            indexes.append(
                DbIndex(
                    tuple(DbIndexField(**f) for f in fds_norm),
                    primary=primary,
                    unique=unique,
                    # Keep the DB-provided name: callers (e.g. drop_index_by_name/get_index_name)
                    # rely on it. Equality ignores name.
                    name=name,
                )
            )

        # psycopg2 treats '%' as placeholder marker; escape it as '%%' in SQL literals
        res = self.query(
            "SELECT column_name, data_type, is_nullable, column_default, character_maximum_length, (column_default LIKE 'nextval%%') AS is_auto_increment "
            "FROM information_schema.columns WHERE table_name = %s ORDER BY ordinal_position",
            (tab,),
        )
        cols = []
        for col in res:
            try:
                if hasattr(col, "column_name"):
                    col_name = col.column_name
                    col_type = col.data_type
                    is_nullable = col.is_nullable
                    col_default = col.column_default
                    char_max_len = getattr(col, "character_maximum_length", None)
                    is_auto_increment = bool(getattr(col, "is_auto_increment", False))
                elif hasattr(col, "__getitem__") and len(col) >= 6:
                    (
                        col_name,
                        col_type,
                        is_nullable,
                        col_default,
                        char_max_len,
                        is_auto_increment,
                    ) = (
                        col[0],
                        col[1],
                        col[2],
                        col[3],
                        col[4],
                        col[5],
                    )
                    is_auto_increment = bool(is_auto_increment)
                elif hasattr(col, "__getitem__") and len(col) >= 4:
                    # Fallback if is_auto_increment column is missing
                    col_name, col_type, is_nullable, col_default = (
                        col[0],
                        col[1],
                        col[2],
                        col[3],
                    )
                    char_max_len = col[4] if len(col) > 4 else None
                    is_auto_increment = False
                else:
                    log.warning("Unexpected column result structure: %s", col)
                    continue
            except (ValueError, IndexError, TypeError) as e:
                log.warning("Error parsing column result: %s, col=%s", e, col)
                continue
            in_primary = col_name in primary_fields
            dbcol = self.column_model(
                col_name,
                col_type,
                is_nullable,
                col_default,
                is_auto_increment,
                in_primary,
                char_max_len,
            )
            cols.append(dbcol)

        if len(set(indexes)) != len(indexes):
            log.warning("duplicate indexes in table %s", tab)
        return DbTable(columns=tuple(cols), indexes=set(indexes))

    @staticmethod
    def simplify_model(model: DbModel):
        model2 = DbModel()
        for nam, tab in model.items():
            primary_fields: Tuple[str, ...] = ()
            for index in tab.indexes:
                if index.primary:
                    primary_fields = tuple(f.name for f in index.fields)
            cols = []
            for col in tab.columns:
                d = col._asdict()
                if col.typ == DbType.INTEGER and not col.size:
                    d["size"] = 8
                # Autoinc integer widths are not faithfully round-trippable (serial vs bigint defaults).
                # Normalize autoinc integers to "unspecified" so they compare against other dialect models.
                if col.typ == DbType.INTEGER and col.autoinc:
                    d["size"] = 8
                # PostgreSQL has no 1-byte integer; normalize tinyint-like size=1 to smallint size=2
                if col.typ == DbType.INTEGER and col.size == 1:
                    d["size"] = 2
                # PostgreSQL has no sized blob type (bytea is unbounded); normalize sizes away.
                if col.typ == DbType.BLOB:
                    d["size"] = 0
                    d["fixed"] = False
                if col.name in primary_fields:
                    d["notnull"] = True
                # Custom column metadata is dialect-specific; ignore for comparisons.
                d["custom"] = None
                col = DbCol(**d)
                cols.append(col)
            # Index names are DB-assigned metadata; ignore them for comparisons (like sqlite does).
            norm_indexes = set()
            for idx in tab.indexes:
                norm_indexes.add(
                    DbIndex(
                        fields=tuple(f._replace(prefix_len=None) for f in idx.fields),
                        unique=False if idx.primary else idx.unique,
                        primary=idx.primary,
                        name=None,
                    )
                )
            model2[nam] = DbTable(columns=tuple(cols), indexes=norm_indexes)

        return model2

    def column_model(
        self,
        col_name,
        col_type,
        is_nullable,
        col_default,
        is_auto_increment,
        in_primary,
        char_max_len=None,
    ):
        # notanorm convention: size=0 means "unspecified/unbounded" for TEXT/BLOB
        size = 0
        fixed = False

        match_t = re.match(r"(character varying|character|text)\((\d+)\)", col_type)
        match_b = re.match(r"(bytea)\((\d+)\)", col_type)

        if match_t:
            typ = DbType.TEXT
            size = int(match_t[2])
        elif match_b:
            typ = DbType.BLOB
        else:
            typ = self._type_map_inverse.get(col_type, DbType.ANY)

        # information_schema for varchar/char returns type without "(n)".
        if typ == DbType.TEXT and (not size) and char_max_len:
            try:
                size = int(char_max_len)
            except Exception:
                pass

        if typ == DbType.INTEGER and col_type in ("smallint", "integer", "bigint"):
            size = {"smallint": 2, "integer": 4, "bigint": 8}[col_type]

        if col_type == "character":
            fixed = True

        custom = None

        autoinc_primary = in_primary and is_auto_increment

        # Normalize defaults: for serial/bigserial, information_schema exposes nextval(...),
        # but notanorm models treat autoinc columns as having no explicit default.
        if is_auto_increment:
            default_norm = None
        else:
            default_norm = col_default
            if isinstance(default_norm, str):
                # strip postgres casts like "'4'::integer"
                default_norm = default_norm.split("::", 1)[0]
                if default_norm.startswith("'") and default_norm.endswith("'"):
                    default_norm = default_norm[1:-1]

        ret = DbCol(
            col_name,
            typ,
            fixed=fixed,
            size=size,
            notnull=is_nullable == "NO" and not autoinc_primary,
            default=default_norm,
            autoinc=is_auto_increment,
            custom=custom,
        )

        return ret

    def version(self):
        return self.query("SELECT version()")[0][0]
