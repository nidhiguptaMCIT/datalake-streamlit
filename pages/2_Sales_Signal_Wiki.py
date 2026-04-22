"""Sales Signal Wiki — local gong-wiki markdown or materialize from Redshift Gong tables."""

from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path

try:
    from dotenv import load_dotenv

    load_dotenv(Path(__file__).resolve().parent.parent / ".env")
except ImportError:
    pass

import os
import shutil

import pandas as pd
import streamlit as st

from db import (
    bootstrap_redshift_env_from_1password_and_aws,
    connect_datalake,
    default_datalake_schema,
    env_ready,
)
from gong_datalake_ingest import (
    default_generated_wiki_dir,
    fetch_calls_for_range,
    fetch_transcripts_for_calls,
    materialize_wiki_tree,
)
from gong_wiki_sources import WikiSourceMeta, filter_by_date_range, iter_source_metas, resolve_wiki_root
from llm_keys import resolve_llm_keys
from nl_sql import default_llm_provider
from wiki_qa import answer_wiki_question


def _provider_for_ui(choice: str) -> str | None:
    return {
        "Auto": None,
        "Claude (Anthropic API)": "anthropic",
        "Claude CLI (experimental)": "claude_cli",
    }[choice]


def _api_key_for_provider(provider: str | None, anthropic_k: str | None) -> str | None:
    p = (provider or default_llm_provider()).lower()
    if p == "claude":
        p = "anthropic"
    if p == "anthropic":
        return anthropic_k
    return None


def _meta_to_row(m: WikiSourceMeta) -> dict[str, str | None]:
    return {
        "call_id": m.call_id,
        "call_date": m.call_date.isoformat(),
        "path": str(m.path),
        "customer": m.customer,
        "outcome": m.outcome,
        "confidence": m.confidence,
        "opportunity_stage": m.opportunity_stage,
    }


def _row_to_meta(r: dict[str, str | None]) -> WikiSourceMeta:
    cd = r.get("call_date") or ""
    return WikiSourceMeta(
        call_id=str(r["call_id"]),
        call_date=date.fromisoformat(str(cd)[:10]),
        path=Path(str(r["path"])),
        customer=(str(r["customer"]) if r.get("customer") else None),
        outcome=(str(r["outcome"]) if r.get("outcome") else None),
        confidence=(str(r["confidence"]) if r.get("confidence") else None),
        opportunity_stage=(str(r["opportunity_stage"]) if r.get("opportunity_stage") else None),
    )


@st.cache_data(ttl=120, show_spinner="Indexing wiki sources…")
def _cached_all_meta_rows(wiki_root_resolved: str) -> tuple[dict[str, str | None], ...]:
    root = Path(wiki_root_resolved)
    return tuple(_meta_to_row(m) for m in iter_source_metas(root))


st.set_page_config(
    page_title="Sales Signal Wiki",
    page_icon="📣",
    layout="wide",
)

st.title("Sales Signal Wiki (Gong)")
st.caption(
    "**Local wiki:** markdown from [gong-wiki](https://github.com/PagerDuty/gong-wiki) (`wiki/sources/`). "
    "**Data Lake:** read-only SQL on `gong_io__call` + `gong_io__call_transcript` → materialize "
    "[Karpathy-style](https://gist.github.com/karpathy/442a6bf555914893e9891c11519de94f) "
    "persistent markdown under `wiki_generated/` (`index.md`, `log.md`, `sources/`)."
)

today = date.today()
default_end = today
default_start = today - timedelta(days=90)

col_a, col_b = st.columns(2)
with col_a:
    start_d = st.date_input("Start date", value=default_start, max_value=today)
with col_b:
    end_d = st.date_input("End date", value=default_end, max_value=today)

source_mode = st.radio(
    "Source",
    ["Local markdown wiki", "Materialize from Data Lake (Redshift)"],
    horizontal=True,
    key="gong_wiki_source_mode",
)

wiki_root: Path | None = None
wiki_path_input = ""

if source_mode.startswith("Local"):
    wiki_path_input = st.text_input(
        "Wiki root (optional)",
        value=os.environ.get("GONG_WIKI_PATH", "").strip(),
        placeholder="Empty = auto: ./wiki, ./wiki_generated, or ../gong-wiki/wiki",
        help="Folder that **contains** `sources/` (not a single .md). You can paste a path to "
        "`.../sources/7903....md` and it will use the parent wiki root.",
    )
    wiki_root = resolve_wiki_root(wiki_path_input or None)
    if wiki_root is None:
        st.warning(
            "No wiki found. Options: (1) Use **Data Lake** to materialize into **`wiki_generated/`**, "
            "then switch back to **Local** — **`wiki_generated`** is auto-detected. "
            "(2) Set **Wiki root** to your wiki folder (parent of `sources/`), e.g. "
            "`/Users/.../datalake-streamlit/wiki_generated`. (3) Clone **gong-wiki** and run **`./setup.sh`**, or "
            "set **`GONG_WIKI_PATH`**."
        )
        st.stop()

    c_wiki, c_refresh = st.columns([4, 1])
    with c_wiki:
        st.success(f"Using wiki: `{wiki_root}`")
    with c_refresh:
        if st.button("Refresh index", help="Clear scan cache after syncing new files from S3"):
            _cached_all_meta_rows.clear()
            st.rerun()

else:
    gen_root = default_generated_wiki_dir()
    st.info(
        f"Writes **`{gen_root}`** (`sources/*.md`, `index.md`, `log.md`). Folder is gitignored. "
        "Uses **`partition_date`** and **`call_date`** on `gong_io__call` per query-datalake SKILL."
    )

    ready, missing = env_ready()
    if not ready:
        with st.spinner("Loading Redshift environment (1Password + AWS)…"):
            _, boot_err = bootstrap_redshift_env_from_1password_and_aws()
        ready, missing = env_ready()
        if not ready:
            st.error(
                "**Redshift not configured.** "
                + (f"{boot_err}\n\n" if boot_err else "")
                + f"Missing: **{', '.join(missing)}**."
            )
            st.stop()

    sch = default_datalake_schema()
    st.caption(f"Schema: **`{sch}`**")

    max_calls = st.slider("Max calls to load", 5, 200, 40, help="Newest calls in range first.")
    max_tr_rows = st.slider("Max transcript rows (total)", 5_000, 200_000, 50_000, step=5_000)

    if st.button("Materialize wiki from Data Lake", type="primary"):
        try:
            with st.spinner("Querying Gong tables…"):
                conn = connect_datalake()
                try:
                    calls_df = fetch_calls_for_range(
                        conn, sch, start_d, end_d, max_calls=max_calls
                    )
                    if calls_df.empty:
                        st.warning("No rows returned for this date range and limits.")
                    else:
                        ids = [str(x).strip() for x in calls_df["call_id"].tolist()]
                        transcripts = fetch_transcripts_for_calls(
                            conn, sch, ids, max_total_rows=max_tr_rows
                        )
                        materialize_wiki_tree(gen_root, calls_df, transcripts)
                        _cached_all_meta_rows.clear()
                        st.success(
                            f"Wrote **{len(calls_df)}** source page(s) under `{gen_root / 'sources'}`."
                        )
                        st.rerun()
                finally:
                    conn.close()
        except Exception as e:
            st.error(f"{type(e).__name__}: {e}")
            st.stop()

    wiki_root = gen_root
    if not (wiki_root / "sources").is_dir() or not any(wiki_root.glob("sources/*.md")):
        st.warning("Click **Materialize wiki from Data Lake** to create sources, then browse and ask below.")
        st.stop()

    if st.button("Clear generated wiki cache (filesystem scan)"):
        _cached_all_meta_rows.clear()
        st.rerun()

# --- shared: index metas, filter by date, table + ask ---

assert wiki_root is not None
all_metas = [_row_to_meta(r) for r in _cached_all_meta_rows(str(wiki_root.resolve()))]
in_range = filter_by_date_range(all_metas, start_d, end_d)

st.metric("Sources in date range", len(in_range), help=f"Frontmatter `date` in **{start_d}** … **{end_d}** (inclusive).")

if not in_range:
    st.info(
        f"No sources in **{start_d}** … **{end_d}**. "
        f"Indexed total: **{len(all_metas)}**. "
        "For Data Lake mode, materialize again if you changed dates."
    )
    st.stop()

df = pd.DataFrame(
    {
        "call_id": [m.call_id for m in in_range],
        "date": [m.call_date.isoformat() for m in in_range],
        "customer": [m.customer or "" for m in in_range],
        "outcome": [m.outcome or "" for m in in_range],
        "confidence": [m.confidence or "" for m in in_range],
        "path": [str(m.path) for m in in_range],
    }
).sort_values("date", ascending=False)

st.dataframe(df, use_container_width=True, height=320, hide_index=True)

with st.expander("Open `index.md` preview (generated / local)"):
    idx = wiki_root / "index.md"
    if idx.is_file():
        st.markdown(idx.read_text(encoding="utf-8", errors="replace")[:12_000])
    else:
        st.caption("No `index.md` (local gong-wiki may omit it).")

st.subheader("Ask the wiki (filtered range)")
st.caption(
    "Claude sees a **sample** of sources (newest first). Same LLM options as the Data Lake home page."
)

max_for_llm = st.slider(
    "Max source files to send to the model",
    min_value=1,
    max_value=min(40, len(in_range)),
    value=min(12, len(in_range)),
    key="wiki_max_llm",
)

chars_per = st.slider("Max characters per source excerpt", 1500, 8000, 4500, step=500, key="wiki_chars")

question = st.text_area(
    "Question",
    height=100,
    placeholder='e.g. "What pain points show up most in these calls?"',
    key="wiki_q",
)

c1, c2 = st.columns([2, 2])
with c1:
    llm_choice = st.selectbox(
        "LLM",
        ["Auto", "Claude (Anthropic API)", "Claude CLI (experimental)"],
        index=0,
        key="wiki_llm_choice",
    )
with c2:
    claude_model = st.text_input(
        "Claude model (API only)",
        value=os.getenv("ANTHROPIC_MODEL", "claude-sonnet-4-20250514"),
        key="wiki_claude_model",
    )

anthropic_k = resolve_llm_keys()
if not anthropic_k and not shutil.which("claude"):
    st.warning("Set **ANTHROPIC_API_KEY** or install **Claude Code** so `claude` is on PATH.")

run_qa = st.button("Ask", type="primary", key="wiki_ask_btn")

if run_qa:
    if not question.strip():
        st.warning("Enter a question.")
    else:
        sorted_range = sorted(in_range, key=lambda m: m.call_date, reverse=True)
        sample = sorted_range[:max_for_llm]

        prov = _provider_for_ui(llm_choice)
        resolved = (prov or default_llm_provider()).lower()
        if resolved == "claude":
            resolved = "anthropic"

        if resolved == "claude_cli":
            model = None
        else:
            model = claude_model.strip() or None

        key = _api_key_for_provider(prov, anthropic_k)
        try:
            with st.spinner(f"Asking Claude over {len(sample)} source(s)…"):
                answer = answer_wiki_question(
                    question,
                    sample,
                    provider=prov,
                    api_key=key,
                    model=model,
                    max_chars_per_source=chars_per,
                )
            st.markdown(answer)
            with st.expander("Source call_ids included in this answer"):
                st.write(", ".join(m.call_id for m in sample))
        except Exception as e:
            st.error(f"{type(e).__name__}: {e}")
