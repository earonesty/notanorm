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


class PostgresDb(DbBase):
    uri_name = "postgres"

    placeholder = "%s"
    default_values = " DEFAULT NULL"
    max_index_name = 63  # PostgreSQL has a 63 character limit for index names

    def _begin(self, conn):
        conn.cursor().execute("BEGIN")

    @classmethod
    def uri_adjust(cls, args, kws):
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
        if not setvals:
            fields = self.primary_fields(table)
            f0 = next(iter(fields))
            return inssql + f" ON CONFLICT ({f0}) DO NOTHING", insvals
        return (
            inssql + f" ON CONFLICT ({', '.join(setvals)}) DO UPDATE SET {setsql}",
            insvals,
        )

    @staticmethod
    def translate_error(exp):
        return exp  # You can add PostgreSQL-specific error handling here

    def _connect(self, *args, **kws):
        conn = psycopg2.connect(*args, **kws)
        conn.autocommit = True
        return conn

    def _cursor(self, conn):
        return conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    @classmethod
    def quote_key(cls, key):
        return sql.Identifier(key)

    def _get_primary(self, table):
        info = self.query(
            """
            SELECT a.attname
            FROM pg_index i
            JOIN pg_attribute a ON a.attnum = ANY(i.indkey)
            WHERE i.indrelid = %s AND i.indisprimary;
            """,
            (self.quote_key(table),),
        )
        prim = set()
        for x in info:
            prim.add(x[0])
        return prim

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
    _type_map_custom = {
        "text": DbColCustomInfo("postgres", "text"),
        "bytea": DbColCustomInfo("postgres", "bytea"),
    }

    def create_table(
        self, name, schema: DbTable, ignore_existing=False, create_indexes: bool = True
    ):
        coldefs = []
        primary_fields: Tuple[str, ...] = ()
        for idx in schema.indexes:
            if idx.primary:
                primary_fields = tuple(f.name for f in idx.fields)

        for col in schema.columns:
            coldef = sql.Identifier(col.name)
            if (
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
                typ = sql.SQL("varchar({})").format(sql.Identifier(col.size))
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
                coldef += sql.SQL(" DEFAULT ") + sql.Identifier(col.default)
            if col.autoinc:
                if (col.name,) != primary_fields:
                    raise err.SchemaError(
                        f"auto increment only works on primary key: {col.name}"
                    )
                coldef += sql.SQL(" SERIAL")
            coldefs.append(coldef)
        keys = [sql.Identifier(k) for k in primary_fields]
        if keys:
            coldefs.append(sql.SQL("PRIMARY KEY ({})").format(sql.SQL(", ").join(keys)))

        ignore = "IF NOT EXISTS " if ignore_existing else ""
        create = sql.SQL("CREATE TABLE {} ({});").format(
            sql.Identifier(name), sql.SQL(", ").join(coldefs)
        )
        self.query(create)

        if create_indexes:
            self.create_indexes(name, schema)

    def _create_index(self, table_name, index_name, idx):
        unique = "UNIQUE " if idx.unique else ""
        icreate = sql.SQL("CREATE {}INDEX {} ON {} ({})").format(
            sql.SQL(unique),
            sql.Identifier(index_name),
            sql.Identifier(table_name),
            sql.SQL(", ").join(
                sql.Identifier(f.name)
                if f.prefix_len is None
                else sql.SQL(f"{f.name}({f.prefix_len})")
                for f in idx.fields
            ),
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
        sql = sql.SQL("DROP INDEX IF EXISTS {} ON {}").format(
            sql.Identifier(index_name), sql.Identifier(table)
        )
        self.execute(sql)

    def table_model(self, tab, no_capture):
        res = self.query(
            "SELECT indexname, indexdef FROM pg_indexes WHERE tablename = %s",
            (tab,),
            no_capture=no_capture,
        )

        idxunique = {}
        idxmap: Dict[str, List[Dict[str, Any]]] = defaultdict(lambda: [])
        for idxinfo in res:
            indexdef = idxinfo[1]
            unique = " UNIQUE " in indexdef
            idxname = idxinfo[0]
            idxunique[idxname] = unique

            match = re.search(r"\((.*?)\)", indexdef)
            if match:
                fields_str = match.group(1)
                field_names = [f.strip() for f in fields_str.split(",")]
                idxmap[idxname] = [{"name": f, "prefix_len": None} for f in field_names]

        indexes = []
        for name, fds in idxmap.items():
            primary = name == "PRIMARY"
            unique = idxunique.get(name, False)
            indexes.append(
                DbIndex(
                    tuple(DbIndexField(**f) for f in fds),
                    primary=primary,
                    unique=unique,
                    name=name,
                )
            )

        res = self.query(
            "SELECT column_name, data_type, is_nullable, column_default, column_default LIKE 'nextval%' AS is_auto_increment FROM information_schema.columns WHERE table_name = %s",
            (tab,),
        )
        cols = []
        for col in res:
            col_name, col_type, is_nullable, col_default, is_auto_increment = col
            in_primary = col_name in primary_fields
            dbcol = self.column_model(
                col_name,
                col_type,
                is_nullable,
                col_default,
                is_auto_increment,
                in_primary,
            )
            cols.append(dbcol)

        if len(set(indexes)) != len(indexes):
            log.warning("duplicate indexes in table %s", tab)
        return DbTable(columns=tuple(cols), indexes=set(indexes))

    @staticmethod
    def simplify_model(model: DbModel):
        model2 = DbModel()
        primary_fields: Tuple[str, ...] = ()
        for nam, tab in model.items():
            for index in tab.indexes:
                if index.primary:
                    primary_fields = tuple(f.name for f in index.fields)
            cols = []
            for col in tab.columns:
                d = col._asdict()
                if col.typ == DbType.INTEGER and not col.size:
                    d["size"] = 8
                if col.name in primary_fields:
                    d["notnull"] = True
                if col.custom and col.custom.dialect == "postgres":
                    d["custom"] = col.custom
                col = DbCol(**d)
                cols.append(col)
            model2[nam] = DbTable(columns=tuple(cols), indexes=tab.indexes)

        return model2

    def column_model(
        self,
        col_name,
        col_type,
        is_nullable,
        col_default,
        is_auto_increment,
        in_primary,
    ):
        size = None
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

        if col_type == "character":
            fixed = True

        custom = self._type_map_custom.get(col_type, None)

        autoinc_primary = in_primary and is_auto_increment

        ret = DbCol(
            col_name,
            typ,
            fixed=fixed,
            size=size,
            notnull=is_nullable == "NO" and not autoinc_primary,
            default=col_default,
            autoinc=is_auto_increment,
            custom=custom,
        )

        return ret

    def version(self):
        return self.query("SELECT version()")[0][0]
