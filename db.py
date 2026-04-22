"""Redshift connection via local SSH tunnel (localhost:5439).

Loads a 1Password item (same fields as ds-core-pipeline ``query-datalake`` ``setup.sh``)
when env vars are missing. The vault reference is configurable — see ``OP_VAULT_ITEM`` below.
"""

from __future__ import annotations

import json
from typing import Any

import os
import re
import shutil
import socket
import subprocess
import sys
import psycopg2
from psycopg2.extensions import connection as PGConnection

WRITE_KEYWORDS = re.compile(
    r"\b(INSERT|UPDATE|DELETE|DROP|ALTER|CREATE|TRUNCATE|MERGE|GRANT|REVOKE|COPY|UNLOAD)\b",
    re.IGNORECASE,
)

# Base ``op://`` reference for the 1Password item (no secret — only vault/item path).
# - **Local default:** matches ds-core-pipeline ``query-datalake`` / ``setup.sh``; no env needed.
# - **Override:** ``export DATALAKE_OP_VAULT_ITEM='op://Vault/Item Name'`` (e.g. public clone
#   where the repo should not embed your team’s vault name — set once in shell or ``.env``).
# - **Public GitHub:** ship a placeholder as the default here or rely on env-only and document.
_DEFAULT_OP_VAULT_ITEM = "op://Data Science/Redshift env vars for Claude"
OP_VAULT_ITEM = (os.getenv("DATALAKE_OP_VAULT_ITEM") or "").strip() or _DEFAULT_OP_VAULT_ITEM
OP_VARS = (
    "REDSHIFT_DB_USER",
    "REDSHIFT_DB_NAME",
    "REDSHIFT_CLUSTER_ID",
    "REDSHIFT_HOST",
    "REDSHIFT_REGION",
    "REDSHIFT_GW_HOST",
    "REDSHIFT_ENDPOINT",
)

_bootstrap_done_ok = False


def _parse_op_vault_item_ref(ref: str) -> tuple[str, str]:
    """Split ``op://Vault name/Item title`` into vault and item title."""
    s = ref.strip()
    if s.lower().startswith("op://"):
        s = s[5:]
    if "/" not in s:
        raise ValueError(f"expected op://Vault/Item, got {ref!r}")
    vault, item = s.split("/", 1)
    return vault.strip(), item.strip()


def _fields_from_op_item_json(payload: dict[str, Any], want: set[str]) -> dict[str, str]:
    """Collect field values whose ``label`` is in ``want`` (nested anywhere in the JSON)."""

    out: dict[str, str] = {}

    def walk(obj: Any) -> None:
        if isinstance(obj, dict):
            lab = obj.get("label")
            val = obj.get("value")
            if isinstance(lab, str):
                ls = lab.strip()
                if ls in want and val is not None and ls not in out:
                    out[ls] = str(val).strip()
            for v in obj.values():
                walk(v)
        elif isinstance(obj, list):
            for x in obj:
                walk(x)

    walk(payload)
    return out


def _op_read(field: str) -> str:
    ref = f"{OP_VAULT_ITEM}/{field}"
    r = subprocess.run(
        ["op", "read", ref],
        capture_output=True,
        text=True,
        timeout=120,
    )
    if r.returncode != 0:
        err = (r.stderr or r.stdout or "").strip()
        raise RuntimeError(f"op read failed for {field}: {err}")
    return (r.stdout or "").strip()


def load_redshift_env_from_1password() -> tuple[bool, str]:
    """Populate os.environ from 1Password (same fields as setup.sh).

    Uses a **single** ``op item get … --format json`` when possible so 1Password only
    prompts once per bootstrap instead of once per field (seven ``op read`` calls).
    Missing fields are filled with individual ``op read`` as a fallback.
    """
    if not shutil.which("op"):
        return False, "1Password CLI not found. Install: brew install 1password-cli then eval $(op signin)"

    want = set(OP_VARS)
    filled: dict[str, str] = {}

    try:
        vault, item_title = _parse_op_vault_item_ref(OP_VAULT_ITEM)
    except ValueError as e:
        return False, str(e)

    r = subprocess.run(
        [
            "op",
            "item",
            "get",
            item_title,
            "--vault",
            vault,
            "--format",
            "json",
        ],
        capture_output=True,
        text=True,
        timeout=120,
    )
    if r.returncode == 0 and (r.stdout or "").strip():
        try:
            payload = json.loads(r.stdout)
            if isinstance(payload, dict):
                filled = _fields_from_op_item_json(payload, want)
        except json.JSONDecodeError:
            filled = {}

    for var in OP_VARS:
        if var in filled and filled[var]:
            os.environ[var] = filled[var]
            continue
        try:
            os.environ[var] = _op_read(var)
        except Exception as e:
            return False, str(e)

    # Match setup.sh: use prod profile for boto3 unless already set
    if not os.getenv("AWS_PROFILE"):
        os.environ["AWS_PROFILE"] = "prod"

    return True, ""


def fetch_temp_redshift_credentials_into_env() -> tuple[bool, str]:
    """Set REDSHIFT_TEMP_USER / REDSHIFT_TEMP_PASSWORD via boto3 (same as setup.sh)."""
    try:
        db_user = os.environ["REDSHIFT_DB_USER"]
        db_name = os.environ["REDSHIFT_DB_NAME"]
        cluster_id = os.environ["REDSHIFT_CLUSTER_ID"]
        region = os.environ["REDSHIFT_REGION"]
    except KeyError as e:
        return False, f"Missing {e.args[0]}"

    import boto3

    client = boto3.client("redshift", region_name=region)
    resp = client.get_cluster_credentials(
        DbUser=db_user,
        DbName=db_name,
        ClusterIdentifier=cluster_id,
    )
    os.environ["REDSHIFT_TEMP_USER"] = resp["DbUser"]
    os.environ["REDSHIFT_TEMP_PASSWORD"] = resp["DbPassword"]
    return True, ""


def _port_open(host: str, port: int, timeout: float = 0.5) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


def ensure_ssh_tunnel_background() -> tuple[bool, str]:
    """If nothing is listening on localhost:5439, start the same tunnel as setup.sh."""
    if _port_open("127.0.0.1", 5439) or _port_open("localhost", 5439):
        return True, ""

    gw = os.getenv("REDSHIFT_GW_HOST")
    endpoint = os.getenv("REDSHIFT_ENDPOINT")
    if not gw or not endpoint:
        return False, "REDSHIFT_GW_HOST or REDSHIFT_ENDPOINT not set"

    cmd = [
        "ssh",
        "-o",
        "BatchMode=yes",
        "-o",
        "StrictHostKeyChecking=accept-new",
        gw,
        "-L",
        f"localhost:5439:{endpoint}:5439",
        "-N",
        "-f",
    ]
    r = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
    if r.returncode != 0:
        return False, (r.stderr or r.stdout or "ssh tunnel failed").strip()

    if not _port_open("127.0.0.1", 5439):
        return False, "SSH reported success but port 5439 is not open yet"

    return True, ""


def bootstrap_redshift_env_from_1password_and_aws() -> tuple[bool, str]:
    """Load 1Password (same item as setup.sh) → AWS temp Redshift password → SSH tunnel."""
    global _bootstrap_done_ok
    if _bootstrap_done_ok:
        ok, missing = env_ready()
        return ok, ("" if ok else f"Still missing: {', '.join(missing)}")

    if env_ready()[0]:
        _bootstrap_done_ok = True
        return True, ""

    ok, err = load_redshift_env_from_1password()
    if not ok:
        return False, err

    ok, err = fetch_temp_redshift_credentials_into_env()
    if not ok:
        return False, f"1Password OK but AWS Redshift credentials failed: {err}"

    ok, err = ensure_ssh_tunnel_background()
    if not ok:
        return False, f"Secrets OK but SSH tunnel failed: {err}"

    ready, missing = env_ready()
    if not ready:
        return False, f"After bootstrap, still missing: {', '.join(missing)}"

    _bootstrap_done_ok = True
    return True, ""


def require_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        print(f"ERROR: {name} is not set", file=sys.stderr)
        sys.exit(1)
    return value


def validate_read_only_sql(sql: str) -> str:
    stripped = sql.strip().rstrip(";")
    if not stripped:
        raise ValueError("Empty query")
    if WRITE_KEYWORDS.search(stripped):
        m = WRITE_KEYWORDS.search(stripped)
        raise ValueError(f"Write operations are not allowed. Found: {m.group(0).upper()}")
    upper = stripped.upper().lstrip()
    if not (
        upper.startswith("SELECT")
        or upper.startswith("WITH")
        or upper.startswith("SHOW")
        or upper.startswith("EXPLAIN")
    ):
        raise ValueError("Only SELECT, WITH, SHOW, and EXPLAIN are allowed")
    return stripped


def get_temporary_credentials(db_user: str, db_name: str, cluster_identifier: str, region: str):
    import boto3

    client = boto3.client("redshift", region_name=region)
    resp = client.get_cluster_credentials(
        DbUser=db_user,
        DbName=db_name,
        ClusterIdentifier=cluster_identifier,
    )
    return resp["DbUser"], resp["DbPassword"]


def connect_datalake() -> PGConnection:
    """Open a psycopg2 connection to the tunneled Redshift endpoint.

    When ``REDSHIFT_CLUSTER_ID`` / region / db user are set, **refreshes IAM temporary
    passwords** on every connect — stale ``REDSHIFT_TEMP_*`` env vars expire (~15 minutes)
    and cause ``IAM Authentication token has expired`` if reused.
    """
    db_name = require_env("REDSHIFT_DB_NAME")

    if all(
        os.getenv(k)
        for k in (
            "REDSHIFT_DB_USER",
            "REDSHIFT_CLUSTER_ID",
            "REDSHIFT_REGION",
        )
    ):
        try:
            fetch_temp_redshift_credentials_into_env()
        except Exception:
            pass

    temp_user = os.getenv("REDSHIFT_TEMP_USER")
    temp_password = os.getenv("REDSHIFT_TEMP_PASSWORD")

    if temp_user and temp_password:
        user, password = temp_user, temp_password
    else:
        db_user = require_env("REDSHIFT_DB_USER")
        cluster_id = require_env("REDSHIFT_CLUSTER_ID")
        region = require_env("REDSHIFT_REGION")
        user, password = get_temporary_credentials(db_user, db_name, cluster_id, region)

    conn = psycopg2.connect(
        dbname=db_name,
        user=user,
        password=password,
        port=5439,
        host="localhost",
    )
    conn.set_session(autocommit=True)
    with conn.cursor() as cur:
        cur.execute("SET statement_timeout TO 120000")  # milliseconds
    return conn


def env_ready() -> tuple[bool, list[str]]:
    """Return (ok, missing_var_names)."""
    missing: list[str] = []
    if not os.getenv("REDSHIFT_DB_NAME"):
        missing.append("REDSHIFT_DB_NAME")

    has_temp = bool(os.getenv("REDSHIFT_TEMP_USER") and os.getenv("REDSHIFT_TEMP_PASSWORD"))
    if not has_temp:
        for k in ("REDSHIFT_DB_USER", "REDSHIFT_CLUSTER_ID", "REDSHIFT_REGION"):
            if not os.getenv(k):
                missing.append(k)

    return len(missing) == 0, missing


def default_datalake_schema() -> str:
    """Schema for Data Lake tables (query-datalake uses ``data_lake.*``)."""
    return (os.getenv("DATALAKE_SCHEMA") or "data_lake").strip() or "data_lake"


def list_tables_in_schema(conn: PGConnection, schema: str) -> list[str]:
    """List tables in a schema.

    Data Lake tables are **Spectrum / Glue external** — metadata is exposed through
    ``SVV_EXTERNAL_TABLES`` and ``SVV_ALL_TABLES``. ``information_schema`` is often empty
    for those objects. We try several catalog paths in order.
    """
    q_ext_with_db = """
    SELECT DISTINCT tablename
    FROM svv_external_tables
    WHERE LOWER(TRIM(schemaname)) = LOWER(TRIM(%s))
      AND redshift_database_name = current_database()
    ORDER BY LOWER(tablename)
    """
    q_ext = """
    SELECT DISTINCT tablename
    FROM svv_external_tables
    WHERE LOWER(TRIM(schemaname)) = LOWER(TRIM(%s))
    ORDER BY LOWER(tablename)
    """
    q_all_with_db = """
    SELECT DISTINCT table_name
    FROM svv_all_tables
    WHERE LOWER(TRIM(schema_name)) = LOWER(TRIM(%s))
      AND database_name = current_database()
    ORDER BY LOWER(table_name)
    """
    q_all = """
    SELECT DISTINCT table_name
    FROM svv_all_tables
    WHERE LOWER(TRIM(schema_name)) = LOWER(TRIM(%s))
    ORDER BY LOWER(table_name)
    """
    q_info = """
    SELECT table_name
    FROM information_schema.tables
    WHERE table_schema = %s
      AND table_type IN ('BASE TABLE', 'EXTERNAL TABLE')
    ORDER BY LOWER(table_name)
    """
    with conn.cursor() as cur:
        for stmt, params in (
            (q_ext_with_db, (schema,)),
            (q_ext, (schema,)),
            (q_all_with_db, (schema,)),
            (q_all, (schema,)),
        ):
            try:
                cur.execute(stmt, params)
                names = [row[0] for row in cur.fetchall()]
                if names:
                    return names
            except Exception:
                continue
        cur.execute(q_info, (schema,))
        return [row[0] for row in cur.fetchall()]


def list_external_schema_hints(conn: PGConnection, *, limit: int = 80) -> list[str]:
    """Distinct schema names that have external (or any) catalog entries — helps pick the right name."""
    seen: set[str] = set()
    ordered: list[str] = []
    queries = (
        """
        SELECT DISTINCT TRIM(schemaname) AS s
        FROM svv_external_tables
        WHERE schemaname IS NOT NULL
        ORDER BY LOWER(s)
        """,
        """
        SELECT DISTINCT TRIM(schema_name) AS s
        FROM svv_all_tables
        WHERE schema_name IS NOT NULL
          AND database_name = current_database()
        ORDER BY LOWER(s)
        """,
    )
    with conn.cursor() as cur:
        for q in queries:
            try:
                cur.execute(q)
                for (name,) in cur.fetchall():
                    s = (name or "").strip()
                    if s and s not in seen:
                        seen.add(s)
                        ordered.append(s)
                        if len(ordered) >= limit:
                            return ordered
            except Exception:
                continue
    return ordered


def describe_table_columns(
    conn: PGConnection, schema: str, table: str
) -> list[dict[str, Any]]:
    """Column metadata: try ``SVV_EXTERNAL_COLUMNS``, ``SVV_COLUMNS``, then ``information_schema``."""
    q_ext_cols_db = """
    SELECT
        columnname AS column_name,
        columnnum AS ordinal_position,
        external_type AS data_type,
        NULL::bigint AS character_maximum_length,
        NULL::integer AS numeric_precision,
        NULL::integer AS numeric_scale,
        is_nullable,
        part_key
    FROM svv_external_columns
    WHERE LOWER(TRIM(schemaname)) = LOWER(TRIM(%s))
      AND LOWER(TRIM(tablename)) = LOWER(TRIM(%s))
      AND redshift_database_name = current_database()
    ORDER BY columnnum
    """
    q_ext_cols = """
    SELECT
        columnname AS column_name,
        columnnum AS ordinal_position,
        external_type AS data_type,
        NULL::bigint AS character_maximum_length,
        NULL::integer AS numeric_precision,
        NULL::integer AS numeric_scale,
        is_nullable,
        part_key
    FROM svv_external_columns
    WHERE LOWER(TRIM(schemaname)) = LOWER(TRIM(%s))
      AND LOWER(TRIM(tablename)) = LOWER(TRIM(%s))
    ORDER BY columnnum
    """
    q_svv_columns = """
    SELECT
        column_name,
        ordinal_position,
        data_type,
        character_maximum_length,
        numeric_precision,
        numeric_scale,
        is_nullable
    FROM svv_columns
    WHERE database_name = current_database()
      AND LOWER(TRIM(schema_name)) = LOWER(TRIM(%s))
      AND LOWER(TRIM(table_name)) = LOWER(TRIM(%s))
    ORDER BY ordinal_position
    """
    q_info = """
    SELECT
        column_name,
        ordinal_position,
        data_type,
        character_maximum_length,
        numeric_precision,
        numeric_scale,
        is_nullable
    FROM information_schema.columns
    WHERE table_schema = %s AND table_name = %s
    ORDER BY ordinal_position
    """
    params_ext = (schema, table)
    with conn.cursor() as cur:
        for stmt in (q_ext_cols_db, q_ext_cols):
            try:
                cur.execute(stmt, params_ext)
                rows = cur.fetchall()
                if not rows:
                    continue
                names = [d[0] for d in cur.description]
                return [dict(zip(names, row)) for row in rows]
            except Exception:
                continue
        try:
            cur.execute(q_svv_columns, params_ext)
            rows = cur.fetchall()
            if rows:
                names = [d[0] for d in cur.description]
                return [dict(zip(names, row)) for row in rows]
        except Exception:
            pass
        cur.execute(q_info, (schema, table))
        names = [d[0] for d in cur.description]
        return [dict(zip(names, row)) for row in cur.fetchall()]
