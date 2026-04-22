"""Scan local Gong Sales Signal wiki ``wiki/sources/*.md`` and filter by call date.

Matches the layout described in ``gong-wiki`` / ``CLAUDE.md`` (YAML frontmatter with ``date``).
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any, Iterator

import yaml


@dataclass(frozen=True)
class WikiSourceMeta:
    """One source summary page under ``wiki/sources/``."""

    call_id: str
    call_date: date
    path: Path
    customer: str | None
    outcome: str | None
    confidence: str | None
    opportunity_stage: str | None


_FRONTMATTER_RE = re.compile(r"^---\s*\n(.*?)\n---\s*\n", re.DOTALL | re.MULTILINE)


def default_wiki_roots(app_dir: Path | None = None) -> list[Path]:
    """Candidate wiki directories (first existing ``sources`` wins in the UI unless overridden)."""
    base = app_dir or Path(__file__).resolve().parent
    env = (os.environ.get("GONG_WIKI_PATH") or "").strip()
    roots: list[Path] = []
    if env:
        roots.append(Path(env).expanduser())
    roots.append(base / "wiki")
    # Data Lake materialize output (same layout as gong-wiki: wiki_root/sources/*.md)
    roots.append(base / "wiki_generated")
    roots.append(base.parent / "gong-wiki" / "wiki")
    return roots


def resolve_wiki_root(explicit: str | None, app_dir: Path | None = None) -> Path | None:
    """Return first directory that contains ``sources/*.md``."""
    if explicit and explicit.strip():
        p = Path(explicit.strip()).expanduser()
        if (p / "sources").is_dir():
            return p.resolve()
        if p.is_dir() and p.name == "sources":
            return p.resolve().parent
        # Pasted path to a single file: .../wiki_generated/sources/79037....md
        if p.is_file() and p.suffix.lower() == ".md" and p.parent.name == "sources":
            return p.parent.parent.resolve()
        return None

    for root in default_wiki_roots(app_dir):
        if (root / "sources").is_dir():
            return root.resolve()
    return None


def _parse_frontmatter(raw: str) -> tuple[dict[str, Any], str]:
    m = _FRONTMATTER_RE.match(raw)
    if not m:
        return {}, raw
    try:
        fm = yaml.safe_load(m.group(1)) or {}
    except yaml.YAMLError:
        return {}, raw
    if not isinstance(fm, dict):
        return {}, raw
    body = raw[m.end() :]
    return fm, body


def _coerce_date(val: Any) -> date | None:
    if val is None:
        return None
    if isinstance(val, datetime):
        return val.date()
    if isinstance(val, date):
        return val
    if isinstance(val, str):
        s = val.strip()[:10]
        try:
            y, mo, d = int(s[0:4]), int(s[5:7]), int(s[8:10])
            return date(y, mo, d)
        except (ValueError, IndexError):
            return None
    return None


def iter_source_metas(wiki_root: Path) -> Iterator[WikiSourceMeta]:
    """Yield metadata for each ``wiki/sources/*.md`` file."""
    sources = wiki_root / "sources"
    if not sources.is_dir():
        return

    for path in sorted(sources.glob("*.md")):
        stem = path.stem
        try:
            text = path.read_text(encoding="utf-8", errors="replace")
        except OSError:
            continue

        fm, _ = _parse_frontmatter(text)
        call_id = str(fm.get("call_id") or stem).strip()
        call_date = _coerce_date(fm.get("date"))
        if call_date is None:
            continue

        customer = fm.get("customer")
        if customer is not None:
            customer = str(customer).strip() or None

        outcome = fm.get("outcome")
        if outcome is not None:
            outcome = str(outcome).strip() or None

        conf = fm.get("confidence")
        if conf is not None:
            conf = str(conf).strip() or None

        stage = fm.get("opportunity_stage")
        if stage is not None:
            stage = str(stage).strip() or None

        yield WikiSourceMeta(
            call_id=call_id,
            call_date=call_date,
            path=path.resolve(),
            customer=customer,
            outcome=outcome,
            confidence=conf,
            opportunity_stage=stage,
        )


def filter_by_date_range(
    metas: Iterator[WikiSourceMeta] | list[WikiSourceMeta],
    start: date,
    end: date,
) -> list[WikiSourceMeta]:
    """Inclusive filter on ``call_date`` (``start`` .. ``end``)."""
    if start > end:
        start, end = end, start
    rows = list(metas)
    return [m for m in rows if start <= m.call_date <= end]


def read_source_body_excerpt(path: Path, max_chars: int) -> str:
    """Return markdown body (after frontmatter) truncated for LLM context."""
    raw = path.read_text(encoding="utf-8", errors="replace")
    _, body = _parse_frontmatter(raw)
    body = body.strip()
    if len(body) <= max_chars:
        return body
    return body[: max_chars - 20] + "\n… (truncated)"
