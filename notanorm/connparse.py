from typing import Tuple, List, Dict, Any, TypeVar, TYPE_CHECKING, Type
from urllib.parse import urlparse, parse_qs, unquote

import notanorm

if TYPE_CHECKING:
    from notanorm import DbBase

T = TypeVar("T", bound="DbBase")


def _db_uri_style_1(dbstr: str) -> Tuple[str, List[str], Dict[str, Any]]:
    """DB URI parser.

    A db uri is:

    db_type:[args,...][,kw=arg]...

    Two examples:

    sqlite:file.db
    mysql:host=localhost,port=2203,passwd=moonpie,db=stuff
    """

    typ, conn = dbstr.split(":", 1)

    args = []
    kws = {}

    assert chr(0) not in conn

    # escaped commas are not split
    conn = conn.replace("\\,", chr(0))
    for arg in conn.split(","):
        # put commas back
        arg = arg.replace(chr(0), ",")

        # allow escaped equals signs in values
        arg = arg.replace("\\=", chr(0))
        if "=" in arg:
            kw, val = arg.split("=", 1)
            # put equals back
            val = val.replace(chr(0), "=")
            assert kw, "invalid uri"
            kws[kw] = val
        else:
            # put equals back
            arg = arg.replace(chr(0), "=")
            args.append(arg)
    return typ, args, kws


def _db_uri_style_2(dbstr: str) -> Tuple[str, List[str], Dict[str, Any]]:
    """DB URI parser.

    A db uri is:

    db_type://[args&...][&kw=arg]...

    Two examples:

    sqlite://file.db
    mysql://localhost?port=2203&passwd=moonpie&db=stuff
    mysql://localhost:3306/mydb
    postgres://localhost:5432/mydb
    """

    if "//" not in dbstr:
        raise ValueError("invalid style 2 uri")

    res = urlparse(dbstr, allow_fragments=False)

    # 'ParseResult', 'scheme netloc path params query fragment')

    typ = res.scheme

    args = []
    kws = {}

    # netloc can be: "host", "host:port", "user@host:port", etc.
    #
    # Important: use urlparse()'s builtin username/password/hostname/port handling.
    # Manual ":" splitting breaks on user:password@host:port (password gets treated
    # as part of the host).
    if res.netloc:
        netloc_parts = res.netloc.split(",")
        for netloc_part in netloc_parts:
            part_res = urlparse(f"{res.scheme}://{netloc_part}", allow_fragments=False)

            # hostname is already de-bracketed for IPv6; keep as-is
            if part_res.hostname:
                args.append(part_res.hostname)
            elif netloc_part:
                # Fall back to raw netloc part if parsing couldn't produce hostname
                args.append(netloc_part)

            try:
                part_port = part_res.port
            except ValueError:
                part_port = None

            if part_port is not None:
                kws["port"] = str(part_port)

            if part_res.username is not None:
                kws["user"] = unquote(part_res.username)
            if part_res.password is not None:
                kws["password"] = unquote(part_res.password)

    # Parse path to extract database name
    # Path format: /database or /path/to/database
    if res.path:
        # Remove leading slash and use as database name
        db_path = res.path.lstrip("/")
        if db_path:
            kws["database"] = db_path

    # Parse query string for additional kwargs
    query_kws = parse_qs(res.query)
    query_kws = {k: v[0] for k, v in query_kws.items()}
    # Query params override path/extracted values
    kws.update(query_kws)

    return typ, args, kws


def parse_db_uri(dbstr: str) -> Tuple[Type["DbBase"], List[str], Dict[str, Any]]:
    """DB URI parser.


    The first form is easier to type for humans that don't know url syntax.

    The second form is more provably complete or correct.

    Both result in args and kwargs that are passed directly to the connection arguments of the db.

    A db uri can be:

    db_type:[args,...][,kw=arg]...

    or

    db_type://[args&...]?[kw=arg]...


    The dbtype is case-insensitive, and corresponds to the "uri_name" of the associated class.

    If no uri_name is specified, then the class_name is used instead.

    Some examples:

    sqlite:file.db
    mysql:host=localhost,port=2203,passwd=moonpie,db=stuff
    mysql://localhost?port=2203&passwd=moon&amp;pie&db=stuff
    """

    try:
        typ, args, kws = _db_uri_style_2(dbstr)
    except Exception:
        typ, args, kws = _db_uri_style_1(dbstr)

    driver = notanorm.DbBase.get_driver_by_name(typ)
    if not driver:
        raise ValueError(f"Db type {typ} not supported")

    driver.uri_adjust(args, kws)

    return driver, args, kws


def open_db(dbstr: str, reconnection_args=None) -> "DbBase":
    """Create db instance using a URI-style connection string.

    The first form is easier to type for humans that don't know url syntax.

    The second form is easier to formalize as a URI.

    Both result in args and kwargs that are passed directly to the connection arguments of the db.

    A db uri can be:

    db_type:[args,...][,kw=arg]...

    or

    db_type://[args&...]?[kw=arg]...


    The dbtype is case-insensitive, and corresponds to the "uri_name" of the associated class.

    If no uri_name is specified, then the class.__name__ is used instead.

    Some examples:

    ```
    open_db("sqlite:file.db")
    open_db("mysql:host=localhost,port=2203,passwd=moonpie,db=stuff")
    open_db("mysql://localhost?port=2203&passwd=moon&amp;pie&db=stuff")
    ```
    """

    driver, args, kws = parse_db_uri(dbstr)
    return driver(*args, reconnection_args=reconnection_args, **kws)
