"""Deterministic synthetic fixture for local runs, tests and CI.

It imitates the three sources closely enough to exercise every pipeline path:

- two consecutive CFPB snapshots (new complaints, status changes, a company
  rename and deletions between them);
- a planted scam spike for one bank, which must raise an alert;
- a company-wide complaint flood for another bank, which must not;
- small FDIC and Census files in the real response formats.

All complaint text values are real CFPB category labels; the complaints are invented.
"""

from __future__ import annotations

import csv
import io
import json
import os
import random
import zipfile
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

from complaint_canary.ingest.cfpb import EXPECTED_HEADER

DAY1 = date(2026, 9, 30)
DAY2 = date(2026, 10, 1)
FIRST_WEEK = date(2026, 3, 2)
SPIKE_WEEK = date(2026, 9, 7)
FLOOD_WEEK = date(2026, 8, 24)
SPIKE_COMPANY = "JPMORGAN CHASE & CO."
FLOOD_COMPANY = "WELLS FARGO & COMPANY"
RENAMED_FROM = "Paypal Holdings, Inc"
RENAMED_TO = "PayPal Holdings, Inc."

STATES = ["TX", "FL", "CA", "NY", "GA"]
STATE_WEIGHTS = [30, 25, 20, 15, 10]

SCAM = ("Money transfer, virtual currency, or money service", "Domestic (US) money transfer", "Fraud or scam", "")
UNAUTHORIZED = (
    "Money transfer, virtual currency, or money service",
    "Mobile or digital wallet",
    "Unauthorized transactions or other transaction problem",
    "",
)
ACCOUNT = ("Checking or savings account", "Checking account", "Managing an account", "Deposits and withdrawals")
CARD_FRAUD = (
    "Credit card",
    "General-purpose credit card or charge card",
    "Getting a credit card",
    "Card opened as result of identity theft or fraud",
)
CREDIT_REPORT = (
    "Credit reporting or other personal consumer reports",
    "Credit reporting",
    "Incorrect information on your report",
    "Account status incorrect",
)
LEGACY_FRAUD = ("Bank account or service", "Checking account", "Identity theft / Fraud / Embezzlement", "")

# company -> list of (category, complaints per week)
WEEKLY_MIX: dict[str, list[tuple[tuple[str, str, str, str], int]]] = {
    "BANK OF AMERICA, NATIONAL ASSOCIATION": [(SCAM, 10), (UNAUTHORIZED, 7), (ACCOUNT, 15)],
    SPIKE_COMPANY: [(SCAM, 9), (UNAUTHORIZED, 7), (ACCOUNT, 15), (CARD_FRAUD, 3)],
    FLOOD_COMPANY: [(SCAM, 8), (UNAUTHORIZED, 6), (ACCOUNT, 15)],
    "Coinbase, Inc.": [(SCAM, 8)],
    "Block, Inc.": [(UNAUTHORIZED, 6)],
    RENAMED_FROM: [(SCAM, 5)],
    "SYNCHRONY FINANCIAL": [(CARD_FRAUD, 6)],
    "Sutton Bank": [(ACCOUNT, 4)],
    "Example Unreviewed Lender LLC": [(ACCOUNT, 2)],
    "EQUIFAX, INC.": [(CREDIT_REPORT, 40)],
}

FDIC_RECORDS = [
    (
        628,
        "JPMorgan Chase Bank, National Association",
        "JPMORGAN CHASE&CO",
        "Columbus",
        "OH",
        4091315000,
        2600000000,
        "N",
    ),
    (
        3510,
        "Bank of America, National Association",
        "BANK OF AMERICA CORP",
        "Charlotte",
        "NC",
        2654645000,
        2000000000,
        "N",
    ),
    (
        3511,
        "Wells Fargo Bank, National Association",
        "WELLS FARGO&COMPANY",
        "Sioux Falls",
        "SD",
        1907928000,
        1400000000,
        "N",
    ),
    (27314, "Synchrony Bank", "SYNCHRONY FINANCIAL", "Draper", "UT", 115217000, 82000000, "SB"),
    (5962, "Sutton Bank", "SUTTON BANCSHARES INC", "Attica", "OH", 1740659, 1500000, "NM"),
    (57203, "Barclays Bank Delaware", "BARCLAYS PLC", "Wilmington", "DE", 43454000, 30000000, "NM"),
]

CENSUS_STATES = [
    ("48", "Texas", 29_200_000),
    ("12", "Florida", 21_600_000),
    ("06", "California", 39_500_000),
    ("36", "New York", 20_100_000),
    ("13", "Georgia", 10_700_000),
]


def _rows_for_week(rng: random.Random, week_start: date, last_day: date) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for company, mix in WEEKLY_MIX.items():
        for category, per_week in mix:
            count = per_week + rng.choice([-1, 0, 0, 1])
            if company == SPIKE_COMPANY and week_start == SPIKE_WEEK and category is SCAM:
                count = per_week * 5
            if company == FLOOD_COMPANY and week_start == FLOOD_WEEK:
                count = per_week * 3
            for _ in range(max(count, 0)):
                received = week_start + timedelta(days=rng.randrange(7))
                if received > last_day:
                    continue
                rows.append(_row(rng, company, category, received))
    return rows


def _row(rng: random.Random, company: str, category: tuple[str, str, str, str], received: date) -> dict[str, str]:
    product, sub_product, issue, sub_issue = category
    return {
        "Date received": received.isoformat(),
        "Product": product,
        "Sub-product": sub_product,
        "Issue": issue,
        "Sub-issue": sub_issue,
        "Company public response": "",
        "Company": company,
        "State": rng.choices(STATES, STATE_WEIGHTS)[0],
        "ZIP code": f"{rng.randrange(10000, 99999)}",
        "Tags": rng.choices(["", "Older American", "Servicemember"], [85, 10, 5])[0],
        "Submitted via": rng.choices(["Web", "Phone", "Referral"], [90, 6, 4])[0],
        "Date sent to company": received.isoformat(),
        "Company response to consumer": "Closed with explanation",
        "Timely response?": rng.choices(["Yes", "No"], [97, 3])[0],
        "Complaint ID": "",
    }


def build_day1() -> list[dict[str, str]]:
    rng = random.Random(20260930)
    rows = [
        _row(rng, "BANK OF AMERICA, NATIONAL ASSOCIATION", LEGACY_FRAUD, date(2016, 5, 2) + timedelta(days=i))
        for i in range(20)
    ]
    week = FIRST_WEEK
    while week <= DAY1:
        rows.extend(_rows_for_week(rng, week, DAY1 - timedelta(days=1)))
        week += timedelta(days=7)
    rows.sort(key=lambda r: r["Date received"])
    for offset, row in enumerate(rows):
        row["Complaint ID"] = str(10_000_001 + offset)
    for row in rows[-40:]:
        row["Company response to consumer"] = "In progress"
    return rows


def build_day2(day1: list[dict[str, str]]) -> list[dict[str, str]]:
    """Day 1 plus: 30 new complaints, 12 closed, 3 renamed, 2 deleted."""
    rng = random.Random(20261001)
    rows = [dict(row) for row in day1]
    del rows[100]
    del rows[200]
    in_progress = [row for row in rows if row["Company response to consumer"] == "In progress"]
    for row in in_progress[:12]:
        row["Company response to consumer"] = "Closed with explanation"
        row["Company public response"] = "Company believes it acted appropriately as authorized by contract or law"
    renamed = [row for row in rows if row["Company"] == RENAMED_FROM][:3]
    for row in renamed:
        row["Company"] = RENAMED_TO
    next_id = 10_000_001 + len(day1)
    for index in range(30):
        # One complaint arrives late, more than 15 days after it was received.
        received = date(2026, 9, 1) if index == 0 else DAY1 - timedelta(days=rng.randrange(2))
        row = _row(rng, "BANK OF AMERICA, NATIONAL ASSOCIATION", SCAM if index % 2 else ACCOUNT, received)
        row["Company response to consumer"] = "In progress"
        row["Complaint ID"] = str(next_id + index)
        rows.append(row)
    return rows


def write_zip(rows: list[dict[str, str]], path: Path, snapshot_date: date) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=EXPECTED_HEADER, lineterminator="\n")
    writer.writeheader()
    writer.writerows(rows)
    with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("complaints.csv", buffer.getvalue())
    # The pipeline reads the snapshot date from the file's modification time.
    stamp = datetime(snapshot_date.year, snapshot_date.month, snapshot_date.day, 9, 15, tzinfo=UTC).timestamp()
    os.utime(path, (stamp, stamp))
    return path


def write_fdic(path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    data = [
        {
            "data": {
                "CERT": cert,
                "NAME": name,
                "NAMEHCR": holding,
                "CITY": city,
                "STALP": state,
                "ASSET": assets,
                "DEP": deposits,
                "BKCLASS": charter,
                "ACTIVE": 1,
                "REPDTE": "06/30/2026",
            }
        }
        for cert, name, holding, city, state, assets, deposits, charter in FDIC_RECORDS
    ]
    path.write_text(json.dumps({"meta": {"total": len(data)}, "data": data}, indent=1))
    return path


def write_census(path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    years = range(2020, 2026)
    with path.open("w", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["SUMLEV", "REGION", "DIVISION", "STATE", "NAME", *[f"POPESTIMATE{year}" for year in years]])
        writer.writerow(
            ["010", "0", "0", "00", "United States", *[331_000_000 + 1_500_000 * i for i in range(len(years))]]
        )
        for fips, name, base in CENSUS_STATES:
            writer.writerow(["040", "3", "7", fips, name, *[base + 150_000 * i for i in range(len(years))]])
    return path


def write_fixture(out_dir: Path) -> dict[str, Path]:
    """Write every fixture file under ``out_dir`` and return their paths."""
    day1 = build_day1()
    return {
        "day1": write_zip(day1, out_dir / "day1" / "complaints.csv.zip", DAY1),
        "day2": write_zip(build_day2(day1), out_dir / "day2" / "complaints.csv.zip", DAY2),
        "fdic": write_fdic(out_dir / "fdic_institutions.json"),
        "census": write_census(out_dir / "census_state_population.csv"),
    }
