from collections import defaultdict
from typing import Tuple, Dict, List, Any, Type

from sqlglot import Expression, parse, exp

from .model import (
    DbType,
    DbCol,
    DbIndex,
    DbTable,
    DbModel,
    ExplicitNone,
    DbIndexField,
    DbColCustomInfo,
)
from .sqlite import SqliteDb
from . import errors as err

import logging

log = logging.getLogger(__name__)


# some support for different sqlglot versions
has_varb = getattr(exp.DataType.Type, "VARBINARY", None)
has_blob = getattr(exp.DataType.Type, "BLOB", None)
has_mediumblob = getattr(exp.DataType.Type, "MEDIUMBLOB", None)
has_longblob = getattr(exp.DataType.Type, "LONGBLOB", None)
has_mediumtext = getattr(exp.DataType.Type, "MEDIUMTEXT", None)
has_longtext = getattr(exp.DataType.Type, "LONGTEXT", None)
has_variant = getattr(exp.DataType.Type, "VARIANT", None)
has_serial = getattr(exp.DataType.Type, "SERIAL", None)
has_smallserial = getattr(exp.DataType.Type, "SMALLSERIAL", None)
has_bigserial = getattr(exp.DataType.Type, "BIGSERIAL", None)


class DDLHelper:
    # map of sqlglot expression types to internal model types
    # Build TYPE_MAP dynamically to support different sqlglot versions
    TYPE_MAP = {
        exp.DataType.Type.INT: DbType.INTEGER,
        exp.DataType.Type.SMALLINT: DbType.INTEGER,
        exp.DataType.Type.TINYINT: DbType.INTEGER,
        exp.DataType.Type.BIGINT: DbType.INTEGER,
        exp.DataType.Type.BOOLEAN: DbType.BOOLEAN,
        exp.DataType.Type.BINARY: DbType.BLOB,
        exp.DataType.Type.VARCHAR: DbType.TEXT,
        exp.DataType.Type.CHAR: DbType.TEXT,
        exp.DataType.Type.TEXT: DbType.TEXT,
        exp.DataType.Type.DECIMAL: DbType.DOUBLE,
        exp.DataType.Type.DOUBLE: DbType.DOUBLE,
        exp.DataType.Type.FLOAT: DbType.FLOAT,
    }

    # Add optional types if they exist in this sqlglot version
    if has_blob:
        TYPE_MAP[exp.DataType.Type.BLOB] = DbType.BLOB
    if has_mediumblob:
        TYPE_MAP[exp.DataType.Type.MEDIUMBLOB] = DbType.BLOB
    if has_longblob:
        TYPE_MAP[exp.DataType.Type.LONGBLOB] = DbType.BLOB
    if has_mediumtext:
        TYPE_MAP[exp.DataType.Type.MEDIUMTEXT] = DbType.TEXT
    if has_longtext:
        TYPE_MAP[exp.DataType.Type.LONGTEXT] = DbType.TEXT
    if has_variant:
        TYPE_MAP[exp.DataType.Type.VARIANT] = DbType.ANY
    if has_varb:
        TYPE_MAP[exp.DataType.Type.VARBINARY] = DbType.BLOB
    if has_serial:
        TYPE_MAP[exp.DataType.Type.SERIAL] = DbType.INTEGER
    if has_smallserial:
        TYPE_MAP[exp.DataType.Type.SMALLSERIAL] = DbType.INTEGER
    if has_bigserial:
        TYPE_MAP[exp.DataType.Type.BIGSERIAL] = DbType.INTEGER

    SIZE_MAP = {
        exp.DataType.Type.TINYINT: 1,
        exp.DataType.Type.SMALLINT: 2,
        exp.DataType.Type.INT: 4,
        exp.DataType.Type.BIGINT: 8,
    }
    if has_serial:
        SIZE_MAP[exp.DataType.Type.SERIAL] = 4
    if has_smallserial:
        SIZE_MAP[exp.DataType.Type.SMALLSERIAL] = 2
    if has_bigserial:
        SIZE_MAP[exp.DataType.Type.BIGSERIAL] = 8

    FIXED_MAP = {
        exp.DataType.Type.CHAR,
    }

    # custom info for weird types and the drivers that might care aboutthem
    CUSTOM_MAP = {}
    if has_mediumtext:
        CUSTOM_MAP[("mysql", exp.DataType.Type.MEDIUMTEXT)] = DbColCustomInfo(
            "mysql", "medium"
        )
    if has_mediumblob:
        CUSTOM_MAP[("mysql", exp.DataType.Type.MEDIUMBLOB)] = DbColCustomInfo(
            "mysql", "medium"
        )
    CUSTOM_MAP[("mysql", exp.DataType.Type.TEXT)] = DbColCustomInfo("mysql", "small")

    def __init__(self, ddl, *dialects, py_defaults=False):
        self.py_defaults = py_defaults

        if not dialects:
            # guess dialect
            dialects = ("mysql", "sqlite")

        first_x = None

        self.__sqlglot = None
        self.__model = None

        if isinstance(ddl, list):
            # preparsed
            self.__sqlglot = ddl
            self.dialect = dialects[0]
            return

        for dialect in dialects:
            try:
                if dialect == "sqlite":
                    self.__model_from_sqlite(ddl)
                else:
                    self.__model_from_sqlglot(ddl, dialect)
                return
            except Exception as ex:
                first_x = first_x or ex

        # earlier (more picky) dialects give better errors
        if first_x:
            raise first_x

    def __model_from_sqlglot(self, ddl, dialect):
        # sqlglot generic parser
        tmp_ddl = ddl
        if dialect == "mysql" and not has_varb:  # pragma: no cover
            # sqlglot 9 doesn't support varbinary
            tmp_ddl = ddl.replace("varbinary", "binary")
            tmp_ddl = ddl.replace("VARBINARY", "BINARY")
        res = parse(tmp_ddl, read=dialect)
        self.__sqlglot = res
        self.dialect = dialect

    def __model_from_sqlite(self, ddl):
        # sqlite memory parser
        ddl = ddl.replace("auto_increment", "autoincrement")
        tmp_db = SqliteDb(":memory:")
        tmp_db.executescript(ddl)
        self.__model = tmp_db.model()
        self.dialect = "sqlite"

    def __columns(self, ent, dialect) -> Tuple[Tuple[DbCol, ...], List[DbIndex]]:
        """Get a tuple of DbCols from a parsed statement

        Argument is a sqlglot parsed grammar of a CREATE TABLE statement.

        If a primary key is specified, return it too.
        """
        cols: List[DbCol] = []
        idxs = []
        for col in ent.find_all(exp.Anonymous):
            if col.name.lower() == "primary key":
                primary_list = [ent.name for ent in col.find_all(exp.Column)]
                idxs.append(
                    DbIndex(
                        fields=tuple(
                            DbIndexField(n, prefix_len=None) for n in primary_list
                        ),
                        primary=True,
                        unique=False,
                    )
                )

        # sqlglot > 11
        for col in ent.find_all(exp.PrimaryKey):
            primary_list = [ent.name for ent in col.find_all(exp.Identifier)]
            idxs.append(
                DbIndex(
                    fields=tuple(
                        DbIndexField(n, prefix_len=None) for n in primary_list
                    ),
                    primary=True,
                    unique=False,
                )
            )

        for col in ent.find_all(exp.ColumnDef):
            dbcol, is_prim, is_uniq = self.__info_to_model(col, dialect)
            if is_prim:
                idxs.append(
                    DbIndex(
                        fields=(DbIndexField(col.name, prefix_len=None),),
                        primary=True,
                        unique=False,
                    )
                )
            elif is_uniq:
                idxs.append(
                    DbIndex(
                        fields=(DbIndexField(col.name, prefix_len=None),),
                        primary=False,
                        unique=True,
                    )
                )
            cols.append(dbcol)
        return tuple(cols), idxs

    @staticmethod
    def __info_to_index(index: Expression, dialect: str) -> Tuple[DbIndex, str]:
        """Get a DbIndex and a table name, given a sqlglot parsed index"""
        primary: exp.PrimaryKeyColumnConstraint = index.find(
            exp.PrimaryKeyColumnConstraint
        )
        unique = index.args.get("unique")

        # Find table - it might be in different places depending on sqlglot version
        tab = index.find(exp.Table)
        if not tab:
            # Try old structure for backward compatibility
            if "this" in index.args and "table" in index.args["this"].args:
                tab = index.args["this"].args["table"]
            else:
                raise err.SchemaError(f"Could not find table in index: {index}")

        # Find columns/expressions - try multiple structures for backward compatibility
        cols = None

        # Try direct access on index first (newest sqlglot versions)
        if hasattr(index, "expressions"):
            cols = index.expressions
        elif hasattr(index, "columns"):
            cols = index.columns
        if cols == []:
            cols = None

        # Try args-based access
        if cols is None and "expressions" in index.args:
            # New structure: columns are directly in expressions
            cols = index.args["expressions"]
        elif cols is None and "columns" in index.args:
            # Alternative: columns directly in args
            cols = index.args["columns"]
        if cols == []:
            cols = None
        elif cols is None and "this" in index.args:
            this_obj = index.args["this"]
            # Check if this_obj has args attribute (newer sqlglot versions)
            if hasattr(this_obj, "args") and isinstance(this_obj.args, dict):
                if "columns" in this_obj.args:
                    cols = this_obj.args["columns"]
                elif "expressions" in this_obj.args:
                    cols = this_obj.args["expressions"]
                # Newer sqlglot: columns may be under params->columns
                params = this_obj.args.get("params")
                if (
                    cols is None
                    and params
                    and hasattr(params, "args")
                    and isinstance(params.args, dict)
                ):
                    if "columns" in params.args:
                        cols = params.args["columns"]
            # Try old structure where columns might be directly accessible
            if cols is None and hasattr(this_obj, "expressions"):
                cols = this_obj.expressions
            elif cols is None and hasattr(this_obj, "columns"):
                cols = this_obj.columns

        # Last resort: try to find all Column or Anonymous expressions in the index
        if cols is None:
            # Search for columns/expressions anywhere in the index structure
            found_cols = []
            for expr in index.find_all(exp.Column):
                found_cols.append(expr)
            for expr in index.find_all(exp.Anonymous):
                found_cols.append(expr)
            if found_cols:
                cols = found_cols

        if cols is None:
            raise err.SchemaError(f"Could not find columns in index: {index}")

        field_info: List[Dict[str, Any]] = []

        # Extract expressions from cols - handle different sqlglot versions
        if isinstance(cols, exp.Tuple):
            if hasattr(cols, "args") and "expressions" in cols.args:
                args = cols.args["expressions"]
            elif hasattr(cols, "expressions"):
                args = cols.expressions
            else:
                args = [cols]
        elif isinstance(cols, list):
            args = cols
        else:
            args = [cols]

        # Unwrap Ordered wrappers (sqlglot >= 28) to get the underlying expression/column
        args = [a.this if isinstance(a, exp.Ordered) else a for a in args]

        # Unwrap Paren expressions - but check if they contain expressions
        unwrapped_args = []
        for a in args:
            if isinstance(a, exp.Paren):
                inner = a.this
                # Check if the inner expression is a function, arithmetic, etc. (not just a column)
                if dialect != "mysql":
                    # For non-mysql, unwrap and check
                    unwrapped_args.append(inner)
                else:
                    # For mysql, check if it's an expression (wrapped in parens means expression)
                    # If it's not a Column or Anonymous, it's an expression
                    if not isinstance(inner, (exp.Column, exp.Anonymous)):
                        raise err.SchemaError("Unsupported type in index definition")
                    unwrapped_args.append(inner)
            else:
                unwrapped_args.append(a)
        args = unwrapped_args

        for ent in args:
            allowed_types: Tuple[Type[Expression], ...]
            if dialect != "mysql":
                # For non-mysql dialects, only Column is allowed
                # Check for function calls, arithmetic, etc.
                if isinstance(ent, exp.Column):
                    # Valid column
                    pass
                else:
                    # It's an expression (function, arithmetic, etc.)
                    if dialect == "sqlite":
                        raise err.SchemaError(
                            "Indices on expressions are currently unsupported"
                        )
                    else:
                        raise err.SchemaError("Unsupported type")
                allowed_types = (exp.Column,)
            else:
                # MySQL prefix indices (e.g. CREATE INDEX ... ON tbl(col(10)))
                # show up as anonymous functions.
                allowed_types = (exp.Column, exp.Anonymous)

            if not isinstance(ent, allowed_types):
                # Check if it's an expression (function call, arithmetic, etc.)
                if dialect != "mysql" and not isinstance(ent, exp.Column):
                    # For non-mysql dialects, expressions are not supported
                    raise err.SchemaError(
                        "Indices on expressions are currently unsupported"
                    )
                else:
                    # For mysql or other cases, use generic error
                    raise err.SchemaError(
                        f"Unsupported type in index definition: {type(ent)}({ent})"
                    )

            if dialect == "mysql" and isinstance(ent, exp.Anonymous):
                # Handle different sqlglot versions for Anonymous expressions
                if hasattr(ent, "args") and "expressions" in ent.args:
                    exps = ent.args["expressions"]
                elif hasattr(ent, "expressions"):
                    exps = ent.expressions
                else:
                    # This might be an expression index, not a prefix index
                    raise err.SchemaError("Unsupported type in index definition")

                # Check if this is a prefix index (col(10)) or an expression
                # Prefix indices have exactly one numeric argument (the length)
                if len(exps) != 1:
                    raise err.SchemaError("Unsupported type in index definition")

                # Check if the Anonymous's 'this' is a Column/Identifier (prefix index) or something else (expression)
                # For prefix indices: Anonymous(this=Identifier/Column("txt"), expressions=[Literal(10)])
                # For expressions: Anonymous(this=Function(...), expressions=[...])
                this_val = None
                if hasattr(ent, "this"):
                    this_val = ent.this
                elif "this" in ent.args:
                    this_val = ent.args["this"]

                if this_val and not isinstance(
                    this_val, (exp.Column, exp.Identifier, str)
                ):
                    # It's an expression, not a prefix index
                    raise err.SchemaError("Unsupported type in index definition")

                # Check if the first expression is a numeric literal (the prefix length)
                first_exp = exps[0]
                if not (isinstance(first_exp, exp.Literal) and not first_exp.is_string):
                    # It's an expression, not a prefix index
                    raise err.SchemaError("Unsupported type in index definition")

                # Try to parse as prefix index - if it fails, it's an expression
                try:
                    # Get the name/value from the expression - handle different structures
                    exp_val = exps[0]
                    if hasattr(exp_val, "name"):
                        prefix_len = int(exp_val.name)
                    elif hasattr(exp_val, "this"):
                        prefix_len = int(exp_val.this)
                    elif hasattr(exp_val, "value"):
                        prefix_len = int(exp_val.value)
                    else:
                        prefix_len = int(str(exp_val))

                    # Get the column name from the Anonymous expression
                    # Try different ways to get the column name
                    col_name = None
                    if hasattr(ent, "this"):
                        # Newer sqlglot: column name might be in 'this'
                        this_val = ent.this
                        if isinstance(this_val, exp.Column):
                            col_name = this_val.name
                        elif isinstance(this_val, exp.Identifier):
                            col_name = this_val.name
                        elif isinstance(this_val, str):
                            col_name = this_val
                        elif hasattr(this_val, "this"):
                            col_name = this_val.this
                        elif hasattr(this_val, "name"):
                            col_name = this_val.name
                        else:
                            col_name = str(this_val)
                    elif hasattr(ent, "name"):
                        col_name = ent.name
                    elif "this" in ent.args:
                        this_val = ent.args["this"]
                        if isinstance(this_val, exp.Column):
                            col_name = this_val.name
                        elif isinstance(this_val, exp.Identifier):
                            col_name = this_val.name
                        elif isinstance(this_val, str):
                            col_name = this_val
                        elif hasattr(this_val, "this"):
                            col_name = this_val.this
                        elif hasattr(this_val, "name"):
                            col_name = this_val.name
                        else:
                            col_name = str(this_val)
                    else:
                        col_name = str(ent)

                    # Normalize to lowercase
                    if col_name:
                        col_name = col_name.lower()
                        field_info.append({"name": col_name, "prefix_len": prefix_len})
                    else:
                        raise err.SchemaError("Unsupported type in index definition")
                except (ValueError, AttributeError, TypeError):
                    # Not a valid prefix index, must be an expression
                    raise err.SchemaError("Unsupported type in index definition")
            else:
                # Get column name - normalize to lowercase for consistency
                col_name = ent.name if hasattr(ent, "name") else str(ent)
                if col_name:
                    col_name = col_name.lower()
                field_info.append({"name": col_name, "prefix_len": None})

        # Ensure we actually captured columns; otherwise it's unsupported
        if not field_info:
            raise err.SchemaError("Unsupported type in index definition")

        return (
            DbIndex(
                fields=tuple(DbIndexField(**f) for f in field_info),
                primary=bool(primary),
                unique=bool(unique),
            ),
            tab.name,
        )

    def __info_to_model(self, info, dialect) -> Tuple[DbCol, bool, bool]:
        """Turn a sqlglot parsed ColumnDef into a model entry."""
        typ = info.find(exp.DataType)
        this = typ and typ.this
        fixed = this in self.FIXED_MAP if this else False
        size = self.SIZE_MAP.get(this, 0) if this else 0
        custom = self.CUSTOM_MAP.get((dialect, this), None) if this else None
        if not this:
            db_typ = DbType.ANY
        else:
            # Handle types that might not exist in all sqlglot versions
            db_typ = self.TYPE_MAP.get(this, DbType.ANY)
        notnull = info.find(exp.NotNullColumnConstraint)
        autoinc = info.find(exp.AutoIncrementColumnConstraint)
        is_primary = info.find(exp.PrimaryKeyColumnConstraint)
        default = info.find(exp.DefaultColumnConstraint)
        is_unique = info.find(exp.UniqueColumnConstraint)

        serial_types = tuple(
            t
            for t in (
                getattr(exp.DataType.Type, "SERIAL", None),
                getattr(exp.DataType.Type, "SMALLSERIAL", None),
                getattr(exp.DataType.Type, "BIGSERIAL", None),
            )
            if t is not None
        )
        is_serial = bool(this and serial_types and this in serial_types)
        autoinc_val = bool(autoinc) or is_serial
        if is_serial:
            size = self.SIZE_MAP.get(this, size)

        # sqlglot has no dedicated or well-known type for the 32 in VARCHAR(32)
        # so this is from the grammar of types:  VARCHAR(32) results in a "type.kind.args.expressions" tuple
        expr = info.args["kind"] and info.args["kind"].args.get("expressions")
        if expr:
            size = int(expr[0].name)

        if default:
            lit = default.find(exp.Literal)
            bool_val = default.find(exp.Boolean)
            if default.find(exp.Null):
                # None means no default, so we have this silly thing
                default = ExplicitNone()
            elif bool_val:
                # None means no default, so we have this silly thing
                default = bool_val.this
            elif not lit:
                default = str(default.this)
            elif lit.is_string:
                default = lit.this
            # this is a hack for compatibility with existing code, todo: change this
            elif lit.is_int and self.py_defaults:
                default = int(lit.output_name)
            elif lit.is_number and self.py_defaults:
                default = float(lit.output_name)
            else:
                default = lit.output_name
        return (
            DbCol(
                name=info.name,
                typ=db_typ,
                notnull=bool(notnull),
                default=default,
                autoinc=autoinc_val,
                size=size,
                fixed=fixed,
                custom=custom,
            ),
            is_primary,
            is_unique,
        )

    def model(self):
        """Get generic db model: dict of tables, each a dict of rows, each with type, unique, autoinc, primary."""
        if self.__model:
            return self.__model

        model = DbModel()
        tabs: Dict[str, Tuple[DbCol, ...]] = {}
        indxs = defaultdict(lambda: [])
        for ent in self.__sqlglot:
            tab = ent.find(exp.Table)
            assert tab, f"unknonwn ddl entry {ent}"
            idx = ent.find(exp.Index)
            if not idx:
                tabs[tab.name], idxs = self.__columns(ent, self.dialect)
                indxs[tab.name] += idxs
            else:
                idx, tab_name = self.__info_to_index(ent, self.dialect)
                indxs[tab_name].append(idx)

        for tab in tabs:
            dbcols: Tuple[DbCol, ...] = tabs[tab]
            model[tab] = DbTable(dbcols, set(indxs[tab]))

        self.__model = model

        return model


def model_from_ddl(ddl: str, *dialects: str) -> DbModel:
    """Convert indexes and create statements to internal model, without needing a database connection."""
    return DDLHelper(ddl, *dialects).model()
