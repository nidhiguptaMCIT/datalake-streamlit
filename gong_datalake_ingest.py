"""Materialize gong-wiki-shaped ``wiki/sources/*.md`` from Redshift Gong tables (read-only).

Uses columns documented in ``ds-core-pipeline`` query-datalake SKILL for
``gong_io__call`` and ``gong_io__call_transcript``. Transcripts are scoped by
``call_id IN (...)`` so we never scan the whole transcript table.
"""

from __future__ import annotations

import re
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import yaml

from db import validate_read_only_sql
from gong_wiki_sources import WikiSourceMeta


def _safe_schema(name: str) -> str:
    n = (name or "").strip()
    if not re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*$", n):
        raise ValueError(f"Invalid schema name: {name!r}")
    return n


def fetch_calls_for_range(
    conn: Any,
    schema: str,
    start: date,
    end: date,
    *,
    max_calls: int,
) -> pd.DataFrame:
    """Return newest calls in ``[start, end]`` on ``call_date``, with ``partition_date`` pruned to same window."""
    if start > end:
        start, end = end, start
    sch = _safe_schema(schema)
    sql = validate_read_only_sql(
        f"""
SELECT
  CAST(call_id AS VARCHAR(65535)) AS call_id,
  CAST(call_date AS DATE) AS call_date,
  CAST(customer_name AS VARCHAR(65535)) AS customer_name,
  CAST(opportunity_stage_name AS VARCHAR(65535)) AS opportunity_stage_name,
  CAST(won_lost_label AS VARCHAR(65535)) AS won_lost_label
FROM {sch}.gong_io__call
WHERE partition_date BETWEEN %s AND %s
  AND CAST(call_date AS DATE) BETWEEN %s AND %s
ORDER BY call_date DESC, call_id DESC
LIMIT %s
"""
    )
    return pd.read_sql(sql, conn, params=[start, end, start, end, int(max_calls)])


def fetch_transcripts_for_calls(
    conn: Any,
    schema: str,
    call_ids: list[str],
    *,
    max_total_rows: int = 80_000,
    max_chars_per_call: int = 120_000,
) -> dict[str, str]:
    """Concatenate ``sentence_text`` per ``call_id`` (ordered by ``sentence_start``)."""
    if not call_ids:
        return {}
    sch = _safe_schema(schema)
    lim = max(1, int(max_total_rows))
    placeholders = ",".join(["%s"] * len(call_ids))
    sql = validate_read_only_sql(
        f"""
SELECT
  CAST(call_id AS VARCHAR(65535)) AS call_id,
  CAST(sentence_text AS VARCHAR(65535)) AS sentence_text,
  sentence_start
FROM {sch}.gong_io__call_transcript
WHERE call_id IN ({placeholders})
ORDER BY call_id, sentence_start
LIMIT {lim}
"""
    )
    df = pd.read_sql(sql, conn, params=tuple(call_ids))

    by_call: dict[str, list[str]] = {}
    for _, row in df.iterrows():
        cid = str(row["call_id"]).strip()
        stx = row.get("sentence_text")
        if stx is not None and str(stx).strip():
            by_call.setdefault(cid, []).append(str(stx).strip())

    joined: dict[str, str] = {}
    for cid in call_ids:
        parts = by_call.get(cid, [])
        text = "\n".join(parts)
        if len(text) > max_chars_per_call:
            text = text[: max_chars_per_call - 30] + "\n… (truncated for size)"
        joined[cid] = text
    return joined


def _frontmatter(
    call_id: str,
    call_date: date,
    customer: str | None,
    stage: str | None,
    outcome: str | None,
) -> str:
    fm = {
        "type": "source",
        "call_id": call_id,
        "date": call_date.isoformat(),
        "customer": customer or "",
        "opportunity_stage": stage or "",
        "outcome": (outcome or "N/A") if outcome else "N/A",
        "confidence": "medium",
        "raw_path": f"data_lake:gong_io__call/{call_id}",
        "tags": ["datalake-ingest"],
    }
    return "---\n" + yaml.safe_dump(fm, default_flow_style=False, allow_unicode=True).strip() + "\n---\n\n"


def _source_body(transcript: str, row: pd.Series) -> str:
    parts = [
        "## Summary",
        "_Generated from Data Lake (`gong_io__call` + `gong_io__call_transcript`). Not a manual gong-wiki ingest._",
        "",
        "## Transcript (concatenated sentences)",
        transcript if transcript.strip() else "_No transcript rows returned for this call._",
    ]
    return "\n".join(parts)


def materialize_wiki_tree(
    output_root: Path,
    calls_df: pd.DataFrame,
    transcript_by_call_id: dict[str, str],
) -> list[WikiSourceMeta]:
    """Write ``sources/{call_id}.md``, ``index.md``, append ``log.md``. Returns metas for written sources."""
    sources_dir = output_root / "sources"
    sources_dir.mkdir(parents=True, exist_ok=True)
    for p in sources_dir.glob("*.md"):
        try:
            p.unlink()
        except OSError:
            pass

    metas: list[WikiSourceMeta] = []
    index_lines: list[str] = [
        "# Sales Signal Wiki (Data Lake)",
        "",
        f"_Generated at {datetime.now(timezone.utc).isoformat()}_",
        "",
        "## Sources",
        "",
    ]

    for _, row in calls_df.iterrows():
        cid = str(row["call_id"]).strip()
        raw_cd = row["call_date"]
        if isinstance(raw_cd, datetime):
            cdate = raw_cd.date()
        elif hasattr(raw_cd, "date") and not isinstance(raw_cd, date):
            cdate = raw_cd.date()  # type: ignore[union-attr]
        elif isinstance(raw_cd, date):
            cdate = raw_cd
        else:
            cdate = date.fromisoformat(str(raw_cd)[:10])

        cust = row.get("customer_name")
        customer = str(cust).strip() if cust is not None and str(cust).strip() else None
        stage_r = row.get("opportunity_stage_name")
        stage = str(stage_r).strip() if stage_r is not None and str(stage_r).strip() else None
        wl = row.get("won_lost_label")
        outcome = str(wl).strip() if wl is not None and str(wl).strip() else None

        trans = transcript_by_call_id.get(cid, "")
        md = _frontmatter(cid, cdate, customer, stage, outcome) + _source_body(trans, row)
        path = sources_dir / f"{cid}.md"
        path.write_text(md, encoding="utf-8")

        metas.append(
            WikiSourceMeta(
                call_id=cid,
                call_date=cdate,
                path=path.resolve(),
                customer=customer,
                outcome=outcome,
                confidence="medium",
                opportunity_stage=stage,
            )
        )
        cust_disp = customer or "—"
        index_lines.append(f"- [[sources/{cid}]] — {cust_disp} — {cdate.isoformat()}")

    index_lines.append("")
    (output_root / "index.md").write_text("\n".join(index_lines), encoding="utf-8")

    log_path = output_root / "log.md"
    log_entry = (
        f"\n## [{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M')}Z] ingest | Data Lake Gong | "
        f"{len(metas)} call(s) | materialized under `sources/`\n"
    )
    if log_path.is_file():
        log_path.write_text(log_path.read_text(encoding="utf-8") + log_entry, encoding="utf-8")
    else:
        log_path.write_text("# Wiki log\n" + log_entry, encoding="utf-8")

    return metas


def default_generated_wiki_dir(app_dir: Path | None = None) -> Path:
    """Directory for lake-materialized wiki (gitignored)."""
    base = app_dir or Path(__file__).resolve().parent
    return (base / "wiki_generated").resolve()
