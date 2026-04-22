"""Claude completions for plain text (not SQL) — Anthropic API or ``claude -p``."""

from __future__ import annotations

import json
import os
import shutil
import subprocess
from typing import Any

from nl_sql import default_llm_provider


def complete_text(
    system: str,
    user: str,
    *,
    provider: str | None = None,
    api_key: str | None = None,
    model: str | None = None,
) -> str:
    """Return assistant text for a single-turn system + user prompt."""
    which = (provider or default_llm_provider()).lower()
    if which == "claude":
        which = "anthropic"

    if which == "anthropic":
        return _complete_anthropic(system, user, api_key=api_key, model=model)
    if which == "claude_cli":
        return _complete_claude_cli(system, user)
    raise ValueError(f"Unknown provider: {which}")


def _complete_anthropic(
    system: str,
    user: str,
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
        system=system,
        messages=[{"role": "user", "content": user.strip()}],
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
    return content


def _complete_claude_cli(system: str, user: str) -> str:
    cli = shutil.which("claude")
    if not cli:
        raise ValueError(
            "Claude CLI not on PATH. Install Claude Code, or set ANTHROPIC_API_KEY for the API."
        )

    prompt = f"{system}\n\n---\n\nUser:\n{user.strip()}"
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
            "Ensure `claude` works on your machine and you are logged in."
        )

    text_out = ""
    try:
        data: dict[str, Any] = json.loads(raw_out)
        text_out = str(data.get("result") or data.get("message") or "").strip()
    except json.JSONDecodeError:
        text_out = raw_out

    if not text_out:
        raise RuntimeError(
            f"Empty response from claude -p. stderr: {raw_err[:1500]!r} stdout: {raw_out[:500]!r}"
        )
    return text_out
