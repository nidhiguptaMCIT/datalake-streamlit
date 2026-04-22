"""Local Data Lake explorer: ask Claude → SQL → local Redshift, or manual SQL."""

from __future__ import annotations

from pathlib import Path

try:
    from dotenv import load_dotenv

    load_dotenv(Path(__file__).resolve().parent / ".env")
except ImportError:
    pass

import os
import shutil
import time
from datetime import datetime, timezone
from typing import Any

import pandas as pd
import streamlit as st

from db import (
    bootstrap_redshift_env_from_1password_and_aws,
    connect_datalake,
    default_datalake_schema,
    describe_table_columns,
    env_ready,
    list_external_schema_hints,
    list_tables_in_schema,
    validate_read_only_sql,
)
from nl_sql import default_llm_provider, natural_language_to_sql
from pipeline_catalog import load_pipeline_skill_tables


@st.cache_data(ttl=300, show_spinner="Loading table catalog…")
def _cached_tables_for_schema(schema: str) -> tuple[str, ...]:
    conn = connect_datalake()
    try:
        return tuple(list_tables_in_schema(conn, schema))
    finally:
        conn.close()


@st.cache_data(ttl=300, show_spinner="Loading columns…")
def _cached_columns(schema: str, table: str) -> list[dict[str, Any]]:
    conn = connect_datalake()
    try:
        return describe_table_columns(conn, schema, table)
    finally:
        conn.close()


@st.cache_data(ttl=300, show_spinner="Loading schema names…")
def _cached_schema_hints() -> tuple[str, ...]:
    conn = connect_datalake()
    try:
        return tuple(list_external_schema_hints(conn))
    finally:
        conn.close()


st.set_page_config(
    page_title="Data Lake (local)",
    page_icon="🧊",
    layout="wide",
    initial_sidebar_state="expanded",
)

MAX_HISTORY = 20
PREVIEW_ROWS = 500
QUERY_TIMEOUT_SEC = 120

_DEFAULT_SQL = (
    "SELECT 1 AS ok\n"
    "-- Or use **Ask & run** above: Claude turns your question into SQL and runs it against local Redshift."
)


def init_session() -> None:
    if "history" not in st.session_state:
        st.session_state.history: list[dict[str, Any]] = []
    if "sql_editor" not in st.session_state:
        st.session_state.sql_editor = _DEFAULT_SQL


def add_history(sql: str, ok: bool, df: pd.DataFrame | None, err: str | None) -> None:
    entry = {
        "ts": datetime.now(timezone.utc).isoformat(),
        "sql": sql,
        "ok": ok,
        "error": err,
        "preview": df.head(PREVIEW_ROWS).copy() if df is not None and not df.empty else None,
        "n_rows": len(df) if df is not None else 0,
    }
    st.session_state.history.insert(0, entry)
    st.session_state.history = st.session_state.history[:MAX_HISTORY]


def run_query(sql: str) -> tuple[pd.DataFrame | None, str | None]:
    try:
        validated = validate_read_only_sql(sql)
    except ValueError as e:
        return None, str(e)

    conn = connect_datalake()
    try:
        with st.spinner(f"Querying Redshift (timeout {QUERY_TIMEOUT_SEC}s)…"):
            df = pd.read_sql(validated, conn)
        return df, None
    except Exception as e:
        return None, f"{type(e).__name__}: {e}"
    finally:
        conn.close()


def _read_streamlit_secrets_anthropic_key() -> str | None:
    """Read Anthropic API key from secrets.toml only if that file exists.

    Direct ``st.secrets[...]`` access without a secrets file makes Streamlit print
    red "No secrets files found" errors — so we use ``load_if_toml_exists()`` first.
    """
    try:
        from streamlit.runtime.secrets import secrets_singleton
    except ImportError:
        return None

    if not secrets_singleton.load_if_toml_exists():
        return None

    try:
        return str(st.secrets["ANTHROPIC_API_KEY"])
    except Exception:
        return None


def resolve_llm_keys() -> str | None:
    """Anthropic API key (env + optional secrets.toml)."""
    a = os.getenv("ANTHROPIC_API_KEY")
    sa = _read_streamlit_secrets_anthropic_key()
    if sa:
        a = a or sa
    return a


def provider_for_ui(choice: str) -> str | None:
    return {
        "Auto": None,
        "Claude (Anthropic API)": "anthropic",
        "Claude CLI (experimental)": "claude_cli",
    }[choice]


def api_key_for_provider(provider: str | None, anthropic_k: str | None) -> str | None:
    p = (provider or default_llm_provider()).lower()
    if p == "claude":
        p = "anthropic"
    if p == "anthropic":
        return anthropic_k
    return None


def main() -> None:
    init_session()

    st.title("Data Lake explorer")
    st.caption(
        "**Ask & run** — uses **Claude Code** (`claude -p`) with your existing login when **`claude`** is on "
        "your PATH (no API key in the app), or **`ANTHROPIC_API_KEY`** for the Claude API. "
        "Then runs SQL on **local Redshift**."
    )

    ready, missing = env_ready()
    if not ready:
        with st.spinner(
            "Loading Redshift settings from 1Password (same item as ds-core-pipeline setup.sh)…"
        ):
            _, boot_err = bootstrap_redshift_env_from_1password_and_aws()
        ready, missing = env_ready()
        if not ready:
            st.error(
                "**Could not load Redshift environment.**\n\n"
                + (f"{boot_err}\n\n" if boot_err else "")
                + f"Missing: **{', '.join(missing)}**.\n\n"
                "**Fix:** `eval $(op signin)`, `aws sso login --profile prod`, or source "
                "`ds-core-pipeline` `setup.sh`.\n"
            )
            st.code(
                "cd /path/to/ds-core-pipeline && source .claude/skills/query-datalake/setup.sh\n"
                "cd /path/to/datalake-streamlit && pip install -r requirements.txt && streamlit run app.py",
                language="bash",
            )
            return
        st.session_state["_show_op_bootstrap_msg"] = True

    if st.session_state.pop("_show_op_bootstrap_msg", False):
        st.success("Redshift tunnel and credentials ready (1Password + AWS).")

    anthropic_k = resolve_llm_keys()

    st.subheader("Ask a question")
    st.caption(
        "**No key needed:** install [Claude Code](https://code.claude.com/docs) and ensure `claude` works in the "
        "same environment you use to start Streamlit (PATH). **Or** set **`ANTHROPIC_API_KEY`** for the Claude API."
    )

    ask_q = st.text_area(
        "Your question",
        height=120,
        placeholder="e.g. Top 10 services by alert count in the last 7 days for account 12345",
        key="ask_question",
        label_visibility="collapsed",
    )

    c1, c2 = st.columns([2, 2])
    with c1:
        llm_choice = st.selectbox(
            "How to generate SQL",
            [
                "Auto",
                "Claude (Anthropic API)",
                "Claude CLI (experimental)",
            ],
            index=0,
            key="llm_choice",
        )
    with c2:
        claude_model = st.text_input(
            "Claude model (optional)",
            value=os.getenv("ANTHROPIC_MODEL", "claude-sonnet-4-20250514"),
            key="claude_model_in",
        )

    ask_run = st.button("Ask & run", type="primary", key="ask_run_btn")

    if ask_run:
        if not ask_q.strip():
            st.warning("Enter a question.")
        else:
            prov = provider_for_ui(llm_choice)
            resolved = (prov or default_llm_provider()).lower()
            if resolved == "claude":
                resolved = "anthropic"

            if resolved == "claude_cli":
                model = None
            else:
                model = claude_model.strip() or None

            key = api_key_for_provider(prov, anthropic_k)
            try:
                with st.spinner("Claude is generating SQL…"):
                    sql_generated = natural_language_to_sql(
                        ask_q,
                        provider=prov,
                        api_key=key,
                        model=model,
                    )
                validate_read_only_sql(sql_generated)
                st.session_state["last_generated_sql"] = sql_generated
                st.session_state.sql_editor = sql_generated

                t0 = time.perf_counter()
                df, err = run_query(sql_generated)
                elapsed = time.perf_counter() - t0

                with st.expander("Generated SQL", expanded=True):
                    st.code(sql_generated, language="sql")

                if err:
                    st.error(err)
                    add_history(sql_generated, False, None, err)
                else:
                    assert df is not None
                    st.success(f"{len(df)} rows · {elapsed:.2f}s")
                    st.dataframe(df, use_container_width=True, height=480)
                    add_history(sql_generated, True, df, None)
            except Exception as e:
                st.error(f"{type(e).__name__}: {e}")

    st.divider()

    with st.expander("Manual SQL (advanced)", expanded=False):
        sql = st.text_area(
            "SQL (SELECT / WITH / SHOW / EXPLAIN only)",
            height=200,
            key="sql_editor",
        )
        col_run, col_clear = st.columns([1, 6])
        with col_run:
            run = st.button("Run SQL only", type="secondary")
        with col_clear:
            if st.button("Clear history"):
                st.session_state.history = []
                st.rerun()

        if run:
            if not sql.strip():
                st.warning("Enter a query.")
            else:
                t0 = time.perf_counter()
                df, err = run_query(sql)
                elapsed = time.perf_counter() - t0
                if err:
                    st.error(err)
                    add_history(sql, False, None, err)
                else:
                    assert df is not None
                    st.success(f"{len(df)} rows · {elapsed:.2f}s")
                    st.dataframe(df, use_container_width=True, height=480)
                    add_history(sql, True, df, None)

    with st.sidebar:
        st.subheader("Tables & columns")
        st.caption(
            "**Pipeline list** comes from **ds-core-pipeline** `query-datalake/SKILL.md` (always available). "
            "**Redshift catalog** (when visible) adds extra names. Schema **`data_lake`** matches the skill."
        )
        sb_schema = st.text_input(
            "Schema",
            value=default_datalake_schema(),
            key="schema_browser_schema",
            help="Redshift schema name, e.g. data_lake",
        )
        if st.button("Refresh catalog", key="schema_browser_refresh"):
            st.cache_data.clear()
            load_pipeline_skill_tables.cache_clear()
            st.rerun()

        schema_name = (sb_schema or "").strip() or default_datalake_schema()

        skill_rows = list(load_pipeline_skill_tables())
        pipeline_set = {r["name"] for r in skill_rows}
        purpose_by_table = {r["name"]: r["purpose"] for r in skill_rows}
        partition_by_table = {r["name"]: r["partition"] for r in skill_rows}

        try:
            catalog_tables = list(_cached_tables_for_schema(schema_name))
        except Exception as e:
            st.error(f"{type(e).__name__}: {e}")
            catalog_tables = []

        extra_from_catalog = sorted(t for t in catalog_tables if t not in pipeline_set)
        if extra_from_catalog:
            st.caption(
                f"**+{len(extra_from_catalog)}** extra table(s) from Redshift (appended in the picker below)."
            )
        elif catalog_tables:
            st.caption("Redshift catalog lists the same tables as the pipeline doc (no extra names).")

        filter_text = st.text_input(
            "Filter tables",
            "",
            key="schema_browser_filter",
            placeholder="substring match…",
        )
        ft = (filter_text or "").strip().lower()

        skill_df = pd.DataFrame(
            {
                "Table": [r["name"] for r in skill_rows],
                "Partition": [r["partition"] for r in skill_rows],
                "Purpose": [r["purpose"][:120] + ("…" if len(r["purpose"]) > 120 else "") for r in skill_rows],
            }
        )
        if ft:
            mask = skill_df["Table"].str.lower().str.contains(ft, na=False) | skill_df["Purpose"].str.lower().str.contains(
                ft, na=False
            )
            skill_df = skill_df.loc[mask].reset_index(drop=True)

        extra_filtered = [t for t in extra_from_catalog if (not ft or ft in t.lower())]
        if extra_filtered:
            extra_df = pd.DataFrame(
                {
                    "Table": extra_filtered,
                    "Partition": ["—"] * len(extra_filtered),
                    "Purpose": ["(Redshift catalog only — not in SKILL doc)"] * len(extra_filtered),
                }
            )
            display_df = pd.concat([skill_df, extra_df], ignore_index=True)
        else:
            display_df = skill_df

        st.markdown(
            "**ds-core-pipeline (query-datalake)** — scroll the table, then **choose the table** "
            "in the dropdown (row-click selection is unreliable in some Streamlit versions)."
        )
        if display_df.empty:
            st.warning("No tables match the filter.")
            selected_table = None
            try:
                hints = list(_cached_schema_hints())
            except Exception:
                hints = []
            if hints:
                st.caption(
                    "**Redshift catalog schemas:** " + ", ".join(hints[:40]) + (" …" if len(hints) > 40 else "")
                )
        else:
            st.dataframe(
                display_df,
                use_container_width=True,
                height=420,
                hide_index=True,
            )
            table_options = display_df["Table"].tolist()
            cur = st.session_state.get("pipeline_schema_table")
            if cur not in table_options:
                st.session_state.pipeline_schema_table = table_options[0]
            selected_table = st.selectbox(
                "Table for schema (columns below)",
                options=table_options,
                key="pipeline_schema_table",
                help="Drives which table’s columns are loaded from Redshift.",
            )
            st.markdown(f"`{schema_name}.{selected_table}`")

        if selected_table is not None:
            columns_loaded_ok = False
            try:
                col_rows = _cached_columns(schema_name, selected_table)
                columns_loaded_ok = True
            except Exception as e:
                msg = str(e)
                low = msg.lower()
                if "expired" in low or "iam" in low or "authentication" in low:
                    st.warning(
                        "**Redshift connection failed** (often an expired IAM password). "
                        "The app refreshes IAM credentials on each connect — try **Refresh catalog**, "
                        "re-run **`setup.sh`**, or **`aws sso login --profile prod`**. "
                        "Documentation below still works without Redshift."
                    )
                    st.cache_data.clear()
                elif "pg_hba" in low or "ssl" in low:
                    st.warning(
                        "**Redshift rejected the connection** (network / SSL / pg_hba). "
                        "Confirm the SSH tunnel to **localhost:5439** and **`REDSHIFT_DB_NAME`** match your cluster."
                    )
                else:
                    st.error(f"{type(e).__name__}: {e}")
                col_rows = []
            if col_rows:
                st.markdown("**Columns (Redshift catalog)**")
                cdf = pd.DataFrame(col_rows)
                st.dataframe(cdf, use_container_width=True, height=280, hide_index=True)
            elif selected_table in purpose_by_table:
                st.markdown("**Documentation (query-datalake SKILL)**")
                st.caption(
                    f"**Partition:** {partition_by_table.get(selected_table, '—')}  \n"
                    f"{purpose_by_table[selected_table]}"
                )
                if columns_loaded_ok:
                    st.caption("No column metadata from Redshift `svv_*` / `information_schema` for this table.")
            elif columns_loaded_ok:
                st.caption("No column metadata returned from catalog views for this table.")

        st.divider()

        st.subheader("Session memory")
        st.caption(f"Up to {MAX_HISTORY} runs · previews capped at {PREVIEW_ROWS} rows")
        if not anthropic_k and not shutil.which("claude"):
            st.warning(
                "No **`claude`** on PATH and no **ANTHROPIC_API_KEY** — "
                "**Ask & run** needs one of these. Install Claude Code, or set **`ANTHROPIC_API_KEY`**."
            )
        if not st.session_state.history:
            st.info("No queries yet this session.")
        for i, item in enumerate(st.session_state.history):
            label = f"{i + 1}. {item['ts'][:19]} · {item['n_rows']} rows"
            with st.expander(label, expanded=(i == 0)):
                st.code(item["sql"][:2000], language="sql")
                if item.get("preview") is not None:
                    st.dataframe(item["preview"], use_container_width=True, height=220)
                if item.get("error"):
                    st.error(item["error"])
                if st.button("Load SQL", key=f"load_{i}"):
                    st.session_state.sql_editor = item["sql"]
                    st.rerun()


if __name__ == "__main__":
    main()
