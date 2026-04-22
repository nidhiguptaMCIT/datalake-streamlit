"""Sales Signal Wiki — browse Gong wiki sources by date range and ask grounded questions."""

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
    "Browse **local** wiki source summaries from the [gong-wiki](https://github.com/PagerDuty/gong-wiki) layout "
    "(`wiki/sources/*.md`). Run **`./setup.sh`** in `gong-wiki` first, or set **`GONG_WIKI_PATH`** to your `wiki` folder."
)

today = date.today()
default_end = today
default_start = today - timedelta(days=90)

col_a, col_b, col_c = st.columns([1, 1, 2])
with col_a:
    start_d = st.date_input("Start date", value=default_start, max_value=today)
with col_b:
    end_d = st.date_input("End date", value=default_end, max_value=today)

wiki_path_input = st.text_input(
    "Wiki root (optional)",
    value=os.environ.get("GONG_WIKI_PATH", "").strip(),
    placeholder="Leave empty to auto-detect: ./wiki or ../gong-wiki/wiki",
    help="Directory that contains a `sources` subfolder (same as gong-wiki after setup.sh).",
)

wiki_root = resolve_wiki_root(wiki_path_input or None)
if wiki_root is None:
    st.warning(
        "No wiki found. Clone **gong-wiki**, run **`./setup.sh`** (AWS profile **prod**), "
        "or set **`GONG_WIKI_PATH`** to the directory containing **`sources`**."
    )
    st.stop()

c_wiki, c_refresh = st.columns([4, 1])
with c_wiki:
    st.success(f"Using wiki: `{wiki_root}`")
with c_refresh:
    if st.button("Refresh index", help="Clear scan cache after syncing new files from S3"):
        _cached_all_meta_rows.clear()
        st.rerun()

all_metas = [_row_to_meta(r) for r in _cached_all_meta_rows(str(wiki_root.resolve()))]
in_range = filter_by_date_range(all_metas, start_d, end_d)

st.metric("Sources in date range", len(in_range), delta=None, help=f"Calls with frontmatter date between {start_d} and {end_d} (inclusive).")

if not in_range:
    st.info(
        f"No source pages with a parsable **`date`** in **{start_d}** … **{end_d}**. "
        f"Total indexed sources (any date): **{len(all_metas)}**."
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

st.dataframe(df, use_container_width=True, height=360, hide_index=True)

st.subheader("Ask the wiki (filtered range)")
st.caption(
    "The model only sees a **sample** of files from your range (to fit context limits). "
    "Same LLM options as the Data Lake page: **API key** or **`claude`** on PATH."
)

max_for_llm = st.slider(
    "Max source files to send to the model",
    min_value=1,
    max_value=min(40, len(in_range)),
    value=min(12, len(in_range)),
    help="Newest calls in range are prioritized.",
)

chars_per = st.slider("Max characters per source excerpt", 1500, 8000, 4500, step=500)

question = st.text_area(
    "Question",
    height=100,
    placeholder='e.g. "What pain points show up most in these calls?"',
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
