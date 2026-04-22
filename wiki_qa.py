"""Grounded Q&A over Gong wiki source excerpts (aligned with gong-wiki CLAUDE.md spirit)."""

from __future__ import annotations

from gong_wiki_sources import WikiSourceMeta, read_source_body_excerpt
from wiki_llm import complete_text

GROUNDING_SYSTEM_PROMPT = """You are answering questions using **only** the Gong Sales Signal wiki source excerpts in the user message. These are structured summaries of sales calls — not the full transcripts.

Non-negotiable rules:
1. **Citations:** When you state what a customer said or implied, cite the call using the bracket form shown in each excerpt header, e.g. `[call_id:CALL123]`.
2. **Quotes:** Prefer short **verbatim** excerpts from the provided text (use Markdown blockquotes). Do not invent quotes or calls.
3. **Gaps:** If the excerpts do not support an answer, say you cannot see that in the selected date range / sample and suggest narrowing the question or widening the date range (user controls dates in the app).
4. **Counts:** Only claim "N customers" or similar if the excerpts clearly support it; otherwise describe patterns qualitatively.
5. **Temporal:** The user chose a date range; remind them conclusions apply to calls in that window only.
"""


def build_user_message(
    question: str,
    metas: list[WikiSourceMeta],
    *,
    max_chars_per_source: int = 5000,
) -> str:
    blocks: list[str] = [
        f"**Question:** {question.strip()}",
        "",
        f"**Sources in scope ({len(metas)} file(s)):**",
        "",
    ]
    for m in metas:
        hdr = (
            f"### [call_id:{m.call_id}] · {m.call_date.isoformat()}"
            + (f" · {m.customer}" if m.customer else "")
            + (f" · outcome={m.outcome}" if m.outcome else "")
            + (f" · confidence={m.confidence}" if m.confidence else "")
        )
        excerpt = read_source_body_excerpt(m.path, max_chars_per_source)
        blocks.append(hdr)
        blocks.append(excerpt)
        blocks.append("")
    return "\n".join(blocks).strip()


def answer_wiki_question(
    question: str,
    metas: list[WikiSourceMeta],
    *,
    provider: str | None = None,
    api_key: str | None = None,
    model: str | None = None,
    max_chars_per_source: int = 5000,
) -> str:
    """Single-turn Claude completion over the given source files."""
    user = build_user_message(question, metas, max_chars_per_source=max_chars_per_source)
    return complete_text(
        GROUNDING_SYSTEM_PROMPT,
        user,
        provider=provider,
        api_key=api_key,
        model=model,
    )
