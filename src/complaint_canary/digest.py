"""Render the weekly scam-signal digest as Markdown."""

from __future__ import annotations

from datetime import date
from typing import Any

CAVEAT = (
    "Complaints are self-reported and lag the events they describe. These are triage "
    "signals for an analyst, not confirmed fraud."
)


def render(alerts: list[dict[str, Any]], snapshot_date: date) -> str:
    week = alerts[0]["week_start"]
    lines = [
        f"# Scam signals for the week of {week}",
        "",
        f"Evaluated on snapshot {snapshot_date}. {len(alerts)} active signal(s).",
        "",
        CAVEAT,
        "",
        "| Severity | Scope | Entity | Fraud category | Complaints | Baseline median | Lift | z-score |",
        "|---|---|---|---|---|---|---|---|",
    ]
    for alert in alerts:
        lines.append(
            f"| {alert['severity']} | {alert['scope']} | {alert['entity_name']} | {alert['fraud_category']}"
            f" | {alert['complaint_count']} | {alert['baseline_median']:.1f} | {alert['lift']:.1f}x"
            f" | {alert['robust_z_score']:.1f} |"
        )
    return "\n".join(lines) + "\n"
