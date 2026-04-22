# datalake-streamlit

Local **Streamlit** UI for read-only SQL against the PagerDuty Data Lake (Redshift), using the same SSH tunnel and environment variables as `ds-core-pipeline`’s `query-datalake` setup.

## Features

- **Data Lake (home page)**: type a **question** → **Claude** generates SQL → the app runs it on **local Redshift** (tunnel) and shows a **table**. Use experimental **`claude` CLI** (`claude -p`) instead of the API when you prefer no API key in the app.
- **Sales Signal Wiki** (sidebar page): browse [gong-wiki](https://github.com/PagerDuty/gong-wiki)-style `wiki/sources/*.md` by **user-chosen start/end dates**, then ask Claude **grounded** questions over a sample of those calls (verbatim excerpts + citations). Point **`GONG_WIKI_PATH`** at your wiki root (folder that contains `sources/`) or keep a `wiki/` next to this app / clone `gong-wiki` alongside and run `./setup.sh` there.
- **Manual SQL**: advanced expander to paste SQL and run without the LLM.
- **Session memory**: sidebar history (SQL + preview, **Load SQL**).
- **Safety**: read-only SQL validation; `statement_timeout` 120s.

### LLM: no API key (Claude Code CLI)

If you already use **`claude`** in the terminal (interactive Claude Code), you do **not** need to put an API key in this app. Install [Claude Code](https://code.claude.com/docs) so **`claude`** is on your **`PATH`**, then start Streamlit from the **same kind of shell** (so it sees `claude`). The app runs:

`claude -p "…" --output-format json`

…which uses your normal Claude Code login (OAuth / keychain). See [headless / `-p` docs](https://code.claude.com/docs/en/headless).

Optional env: **`DATALAKE_CLAUDE_MAX_TURNS`** (default `8`), **`DATALAKE_CLAUDE_TIMEOUT_SEC`** (default `600`).

### LLM: API key (optional)

- **`ANTHROPIC_API_KEY`** — Claude API (no local `claude` binary required). Optional **`ANTHROPIC_MODEL`**.
- **`DATALAKE_LLM=claude_cli`** forces `claude -p` even when an API key is set.

Copy **`.streamlit/secrets.toml.example`** → **`.streamlit/secrets.toml`** if you prefer keys in a file.

Prompt rules match **`ds-core-pipeline`** query-datalake (partition filters, `data_lake.*` tables, etc.).

## Prerequisites

1. **Same access as query-datalake:** 1Password CLI (`op`), AWS SSO profile **`prod`**, and SSH to the Redshift gateway (see `ds-core-pipeline` `.claude/skills/query-datalake/setup.sh`).

2. Install dependencies (use a venv if you like):

   ```bash
   cd /path/to/datalake-streamlit
   python3 -m venv .venv
   source .venv/bin/activate
   pip install -r requirements.txt
   ```

## Run

On first load, the app tries to load Redshift settings **from the same 1Password item** as `setup.sh` (`op://Data Science/Redshift env vars for Claude/...`), then fetches **temporary Redshift credentials** via **boto3** and opens the **SSH tunnel** to `localhost:5439`. You should have run `eval $(op signin)` and `aws sso login --profile prod` recently.

```bash
cd /path/to/datalake-streamlit
streamlit run app.py
```

Open the URL Streamlit prints (usually `http://localhost:8501`).

**Alternative:** start Streamlit after sourcing `setup.sh` in `ds-core-pipeline` so the environment is already set—then the app skips the automatic bootstrap.

## Notes

- Credentials from `setup.sh` expire about every **15 minutes**; re-run `setup.sh` and refresh the app if queries fail to connect.
- History is **not** persisted to disk; it clears when you close the tab or restart Streamlit.
