"""Tables documented in ds-core-pipeline ``query-datalake`` SKILL (schema knowledge).

Used when Redshift ``svv_*`` catalog views are empty or restricted, so the Streamlit UI
still lists every pipeline table with partition / purpose hints from the skill doc.
"""

from __future__ import annotations

import os
import re
from functools import lru_cache
from pathlib import Path
from typing import Any

def _skill_search_paths() -> list[Path]:
    env = os.getenv("DATALAKE_QUERY_DATALAKE_SKILL")
    paths: list[Path] = []
    if env:
        paths.append(Path(env).expanduser().resolve())
    here = Path(__file__).resolve().parent
    paths.append(here.parent / "ds-core-pipeline" / ".claude" / "skills" / "query-datalake" / "SKILL.md")
    return paths


@lru_cache(maxsize=1)
def load_pipeline_skill_tables() -> tuple[dict[str, Any], ...]:
    """Return ordered rows: {name, partition, purpose} for each ``data_lake.*`` table in the SKILL."""
    text: str | None = None
    for p in _skill_search_paths():
        try:
            if p.is_file():
                text = p.read_text(encoding="utf-8")
                break
        except OSError:
            continue
    if text is None:
        return tuple(_embedded_pipeline_rows())

    seen: set[str] = set()
    rows: list[dict[str, Any]] = []
    for line in text.splitlines():
        if "`data_lake." not in line or not line.strip().startswith("|"):
            continue
        parts = [p.strip() for p in line.split("|")]
        if len(parts) < 4:
            continue
        cell = parts[1]
        m = re.search(r"data_lake\.([^`]+)", cell)
        if not m:
            continue
        name = m.group(1).strip()
        if name == "table_name" or name in seen:
            continue
        partition = parts[2].replace("`", "").strip() or "—"
        purpose = " | ".join(parts[3:]).strip().rstrip("|").strip() if len(parts) > 3 else ""
        seen.add(name)
        rows.append(
            {
                "name": name,
                "partition": partition,
                "purpose": purpose,
            }
        )
    if not rows:
        return tuple(_embedded_pipeline_rows())
    return tuple(rows)


def _embedded_pipeline_rows() -> list[dict[str, Any]]:
    """Minimal fallback if SKILL.md is not on disk (same order as query-datalake SKILL)."""
    raw = """
pagerduty_production__alerts|partition_date|Alert records
pagerduty_production__incidents|partition_date|Incidents
pagerduty_production__services|partition_date|Services
pagerduty_production__accounts|—|Accounts
pagerduty_production__users|—|Users
pagerduty_production__teams|—|Teams
pagerduty_production__users_teams|—|User-team mapping
pagerduty_production__services_teams|—|Service-team mapping
pagerduty_production__escalation_policies|—|Escalation policies
pagerduty_production__escalation_rules|—|Escalation rules
pagerduty_production__escalation_targets|partition_date|Escalation targets
pagerduty_production__priorities|—|Priorities
pagerduty_production__incident_priorities|—|Incident priorities
pagerduty_production__notifications|partition_date|Notifications
pagerduty_production__notes|partition_date|Notes
pagerduty_production__schedules|partition_date|Schedules
pagerduty_production__schedule_layers|partition_date|Schedule layers
pagerduty_production__schedule_layer_members|partition_date|Schedule layer members
pagerduty_production__schedule_layer_entries|partition_date|Schedule layer entries
pagerduty_production__channels|—|Channels
pagerduty_production__inbound_integrations|partition_date|Inbound integrations
pagerduty_production__metadata|—|Metadata
pagerduty_production__responder_requests|—|Responder requests
pagerduty_production__status_updates|—|Status updates
pagerduty_production__alerts_restore|partition_date|Alerts restore
pagerduty_production__incidents_merged_incidents|—|Merged incidents
pagerduty_production__incidents_stakeholders|—|Stakeholders
pagerduty_production__incidents_teams|—|Incident teams
les__alert_log_entries|partition_date|Alert log entries
les__incident_log_entries|partition_date|Incident log entries
dataspeedway__ds_raw_events|partition_date|Raw events (dataspeedway)
localpipe__ds_raw_events|partition_date|Raw events (localpipe)
localpipe__event_metering_filtered_events|partition_date|Metered events
localpipe__reified_change_events|partition_date|Change events
localpipe__service_updates|partition_date|Service updates
localpipe__user_updates|partition_date|User updates
alert_grouping_service__alert_groupings|partition_date|Alert groupings
configurable_iag_service__alert_iat_metrics_table|partition_date|IAT metrics
uags__alert_groupings|partition_date|UAGS groupings
ds_auto_pause_model_service__auto_pause_prediction|partition_date|Auto-pause
ds_llm_python_metering__llm_call_details|partition_date|LLM calls
ds_llm_python_metering__request_details|partition_date|LLM requests
eo__event_orchestration|partition_date|Event orchestration
incident_correlation_service__incidents|partition_date|Correlated incidents
incident_correlation_service__business_services|partition_date|Business services
incident_correlation_service__business_to_technical_services|—|Biz-tech mapping
incident_correlation_service__technical_to_technical_services|—|Tech-tech mapping
incident_correlation_service__user_feedbacks|partition_date|Correlation feedback
incident_frequency_service__incidents|partition_date|Frequency incidents
incident_frequency_service__templates|—|Frequency templates
mercury_archive__mercury_messages|partition_date|Mercury messages
mercury_archive__mercury_message_attempts|partition_date|Mercury attempts
mercury_archive__mercury_message_costs|partition_date|Mercury costs
salesforce__opportunity|—|Salesforce opportunities
salesforce__contact|—|Salesforce contacts
salesforce__user|—|Salesforce users
salesforce__user_role|—|Salesforce roles
salesforce__opportunity_line_item|—|Opportunity line items
salesforce__product_2|—|Salesforce products
notification_scheduling_service_archive__event_to_notification_slo_tracking|partition_date|SLO tracking
notification_scheduling_service_archive__slo_tracking_summary|partition_date|SLO summary
user_subscription_management_service__status_update_notifications|—|Status notifications
flex_service__fields|partition_date|Flex fields
flex_service__field_data|partition_date|Flex field data
flex_service__field_schemas|partition_date|Flex schemas
flex_service__field_schema_assignments|partition_date|Schema assignments
flex_service__field_configurations|partition_date|Field configurations
flex_service__field_options|partition_date|Field options
gong_io__call|partition_date|Gong calls
gong_io__call_transcript|—|Gong transcripts
gong_io__speaker|partition_date|Gong speakers
gong_io__tracker|partition_date|Gong trackers
teams__primary_teams|—|Primary teams
user_subscription_management_service__subscriptions|—|Subscriptions
"""
    rows: list[dict[str, Any]] = []
    for line in raw.strip().splitlines():
        parts = line.split("|", 2)
        if len(parts) >= 3:
            rows.append(
                {
                    "name": parts[0].strip(),
                    "partition": parts[1].strip(),
                    "purpose": parts[2].strip(),
                }
            )
    return rows


def clear_pipeline_cache() -> None:
    """Call after setting DATALAKE_QUERY_DATALAKE_SKILL in the same process (tests)."""
    load_pipeline_skill_tables.cache_clear()
