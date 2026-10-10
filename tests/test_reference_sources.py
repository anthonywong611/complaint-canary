from datetime import date

import httpx
import respx

from complaint_canary.ingest import census, fdic

URL = "https://api.fdic.example/banks/institutions"


def _page(certs, total):
    return {
        "meta": {"total": total},
        "data": [
            {"data": {"CERT": c, "NAME": f"Bank {c}", "NAMEHCR": "", "ASSET": 10 * c, "DEP": 5 * c, "STALP": "TX"}}
            for c in certs
        ],
    }


@respx.mock
def test_fdic_follows_pagination_to_the_reported_total():
    route = respx.get(URL).mock(
        side_effect=[
            httpx.Response(200, json=_page([1, 2], total=5)),
            httpx.Response(200, json=_page([3, 4], total=5)),
            httpx.Response(200, json=_page([5], total=5)),
        ]
    )
    with httpx.Client() as client:
        records = fdic.fetch_institutions(URL, client, page_size=2)

    assert [r["CERT"] for r in records] == [1, 2, 3, 4, 5]
    assert [call.request.url.params["offset"] for call in route.calls] == ["0", "2", "4"]


@respx.mock
def test_fdic_retries_a_transient_error(monkeypatch):
    monkeypatch.setattr("tenacity.nap.time.sleep", lambda _: None)
    respx.get(URL).mock(side_effect=[httpx.Response(503), httpx.Response(200, json=_page([1], total=1))])
    with httpx.Client() as client:
        assert len(fdic.fetch_institutions(URL, client)) == 1


def test_fdic_table_treats_a_blank_holding_company_as_missing(fixture_files):
    records = fdic.fetch_institutions(str(fixture_files["fdic"]))
    records.append({"CERT": 99, "NAME": "Standalone Bank", "NAMEHCR": "", "ASSET": 5, "DEP": 4})
    table = fdic.to_table(records, date(2026, 10, 1))

    assert table.num_rows == len(records)
    assert table["holding_company_name"].to_pylist()[-1] is None
    assert table.schema.field("total_assets_thousands").type == "int64"


def test_census_keeps_states_only_and_unpivots_years(fixture_files):
    table = census.parse(fixture_files["census"].read_text(), date(2026, 10, 1))
    rows = table.to_pylist()

    assert {row["state_name"] for row in rows} == {"Texas", "Florida", "California", "New York", "Georgia"}
    assert sorted({row["population_year"] for row in rows}) == [2020, 2021, 2022, 2023, 2024, 2025]
    texas_2025 = next(r for r in rows if r["state_fips"] == "48" and r["population_year"] == 2025)
    assert texas_2025["population"] == 29_950_000
