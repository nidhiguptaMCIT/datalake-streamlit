"""Natural language → Redshift SQL via Claude (Anthropic API or `claude -p` CLI)."""

from __future__ import annotations

import json
import os
import re
import shutil
import subprocess

# Condensed from ds-core-pipeline `.claude/skills/query-datalake/SKILL.md`
SYSTEM_PROMPT = """You are a Redshift SQL expert for the PagerDuty Data Lake. Output exactly one SQL statement, nothing else (no markdown fences unless you must, but prefer raw SQL only).

Hard rules:
1. Only SELECT, WITH, SHOW, or EXPLAIN. Never INSERT/UPDATE/DELETE/DDL/COPY/UNLOAD.
2. Tables are in schema `data_lake` — use fully qualified names like `data_lake.pagerduty_production__alerts`.
3. For any table that has partition_date, you MUST filter `partition_date` with the narrowest reasonable range or the query will scan terabytes and fail. Use CURRENT_DATE and DATEADD. For a **single calendar day**, prefer `partition_date::date = DATEADD(day, k, CURRENT_DATE)::date` (or an equivalent one-day predicate)—avoid useless `BETWEEN d AND d` when both bounds are the same.
4. **Relative dates** ("today", "yesterday", "day before yesterday", "last N days"): interpret as **calendar days** in the lake. For incident/alert **counts by day**, filter **`partition_date` only** for that day (or day range). **Do not** add extra predicates on `created_at`, `resolved_at`, etc. unless the question explicitly asks for "created at" / "resolved at" timing distinct from the partition day—those filters are usually redundant and can skew counts.
5. **"How many incidents/alerts/services"** → use **`COUNT(DISTINCT id)`** (or that table’s primary id column), not `COUNT(*)`, unless the question clearly asks for rows/records. Fact tables can have more than one row per logical entity across partitions or loads.
6. **Multi-tenant scope**: `pagerduty_production__*` tables include **all customers**. With **no `account_id` in the question**, the count is **global** (often millions per day). If the user names an account or says "my / our", add `WHERE account_id = …`. Otherwise add a short trailing SQL **`--`** comment that the result is **all accounts** unless filtered.
7. Use LIMIT on exploratory queries (e.g. 100–500).
8. Redshift has no LEFT ANTI JOIN — use NOT EXISTS or LEFT JOIN ... WHERE right.key IS NULL.
9. For alert_log_entries hex ids vs alerts.id: `STRTOL(RIGHT(big_alert_id, 16), 16)` (not bare STRTOL on full hex).
10. JSON: use JSON_EXTRACT_PATH_TEXT(column, 'key').

Core tables (partition_date where noted):
- data_lake.pagerduty_production__alerts (partition_date) — id, incident_id, account_id, created_at, resolved_at, summary, service_id
- data_lake.pagerduty_production__incidents (partition_date) — id, service_id, account_id, created_at, resolved_at
- data_lake.pagerduty_production__services (partition_date) — id, account_id, name, deleted_at
- data_lake.pagerduty_production__accounts — account dimension
- data_lake.les__alert_log_entries (partition_date) — big_alert_id, type, event_storage_id, properties
- data_lake.les__incident_log_entries (partition_date) — incident_id, type, properties
- data_lake.ds_llm_python_metering__llm_call_details (partition_date) — model, tokens, duration

If the question is ambiguous, pick a reasonable default date range (e.g. last 7 days) and mention it in a trailing SQL comment only.
"""


def _extract_sql(text: str) -> str:
    text = text.strip()
    m = re.search(r"```(?:sql)?\s*([\s\S]*?)```", text, re.IGNORECASE)
    if m:
        return m.group(1).strip()
    return text


def _nl_to_sql_anthropic(
    question: str,
    *,
    api_key: str | None = None,
    model: str | None = None,
) -> str:
    key = api_key or os.getenv("ANTHROPIC_API_KEY")
    if not key:
        raise ValueError("Set ANTHROPIC_API_KEY for Claude (Anthropic API).")

    model_name = model or os.getenv(
        "ANTHROPIC_MODEL",
        "claude-sonnet-4-20250514",
    )

    import anthropic

    client = anthropic.Anthropic(api_key=key)
    msg = client.messages.create(
        model=model_name,
        max_tokens=8192,
        system=SYSTEM_PROMPT,
        messages=[{"role": "user", "content": question.strip()}],
    )
    parts: list[str] = []
    for block in msg.content:
        btype = getattr(block, "type", None)
        if btype == "text" and getattr(block, "text", None):
            parts.append(block.text)
        elif isinstance(block, dict) and block.get("type") == "text":
            parts.append(str(block.get("text", "")))
    content = "".join(parts).strip()
    if not content:
        raise RuntimeError("Empty response from Claude")
    return _extract_sql(content)


def _nl_to_sql_claude_cli(question: str) -> str:
    """Claude Code `claude -p` print mode — uses your normal Claude Code auth (no API key in the app).

    See: https://code.claude.com/docs/en/headless — interactive `claude` is the TUI; `claude -p`
    runs headlessly and reuses OAuth/keychain login. Do **not** use `--bare` here (bare mode
    requires ANTHROPIC_API_KEY).
    """
    cli = shutil.which("claude")
    if not cli:
        raise ValueError(
            "Claude CLI not on PATH. Install Claude Code, or set ANTHROPIC_API_KEY for the API."
        )

    prompt = f"{SYSTEM_PROMPT}\n\nUser question:\n{question.strip()}"
    # --output-format json → parse `.result`; --max-turns caps agent loops for SQL-only tasks
    cmd = [
        cli,
        "-p",
        prompt,
        "--output-format",
        "json",
        "--max-turns",
        os.getenv("DATALAKE_CLAUDE_MAX_TURNS", "8"),
    ]
    proc = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        timeout=int(os.getenv("DATALAKE_CLAUDE_TIMEOUT_SEC", "600")),
        env={**os.environ},
    )
    raw_out = (proc.stdout or "").strip()
    raw_err = (proc.stderr or "").strip()

    if proc.returncode != 0:
        raise RuntimeError(
            f"claude -p failed (exit {proc.returncode}): {(raw_err or raw_out)[:2500]}\n"
            "Ensure `claude` works on your machine and you are logged in (run `claude` once interactively). "
            "See: https://code.claude.com/docs/en/headless"
        )

    text_out = ""
    try:
        data = json.loads(raw_out)
        text_out = str(data.get("result") or data.get("message") or "").strip()
    except json.JSONDecodeError:
        text_out = raw_out

    if not text_out:
        raise RuntimeError(
            f"Empty response from claude -p. stderr: {raw_err[:1500]!r} stdout: {raw_out[:500]!r}"
        )
    return _extract_sql(text_out)


def default_llm_provider() -> str:
    """Prefer Claude Code CLI (no in-app API key) when `claude` is on PATH; else Anthropic API."""
    explicit = (os.getenv("DATALAKE_LLM") or "").strip().lower()
    if explicit in ("anthropic", "claude", "claude_cli"):
        if explicit == "claude":
            return "anthropic"
        return explicit

    if os.getenv("ANTHROPIC_API_KEY"):
        return "anthropic"
    # No API key: use local `claude -p` (Claude Code login) when available
    if shutil.which("claude"):
        return "claude_cli"
    return "anthropic"


def natural_language_to_sql(
    question: str,
    *,
    provider: str | None = None,
    api_key: str | None = None,
    model: str | None = None,
) -> str:
    """Turn a question into SQL via Claude (Anthropic API or `claude -p`).

    Providers: ``anthropic`` (Claude API), ``claude_cli`` (local ``claude`` binary).
    Override with env ``DATALAKE_LLM=anthropic|claude_cli`` (``claude`` is an alias for ``anthropic``).
    """
    which = (provider or default_llm_provider()).lower()
    if which == "claude":
        which = "anthropic"

    if which == "anthropic":
        return _nl_to_sql_anthropic(question, api_key=api_key, model=model)
    if which == "claude_cli":
        return _nl_to_sql_claude_cli(question)
    raise ValueError(f"Unknown provider: {which}")
