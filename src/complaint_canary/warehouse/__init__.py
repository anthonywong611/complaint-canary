from complaint_canary.config import Settings
from complaint_canary.warehouse.base import Warehouse


def get_warehouse(settings: Settings) -> Warehouse:
    if settings.warehouse == "bigquery":
        from complaint_canary.warehouse.bigquery_wh import BigQueryWarehouse

        return BigQueryWarehouse(settings)
    from complaint_canary.warehouse.duckdb_wh import DuckDBWarehouse

    return DuckDBWarehouse(settings)


__all__ = ["Warehouse", "get_warehouse"]
