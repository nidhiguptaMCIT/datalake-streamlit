"""Resolve Anthropic API key from environment and optional Streamlit secrets."""

from __future__ import annotations

import os

import streamlit as st


def read_streamlit_secrets_anthropic_key() -> str | None:
    """Read Anthropic API key from secrets.toml only if that file exists."""
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
    sa = read_streamlit_secrets_anthropic_key()
    if sa:
        a = a or sa
    return a
