# libraries
from __future__ import annotations # makes type hints work for Python versions < 3.10

from pathlib import Path
import duckdb

# Compute KPIs and create supplier_kpis table in DuckDB
def main() -> None:
    project_root = Path(__file__).resolve().parents[1]
    db_path = project_root / "warehouse.db"

    con = duckdb.connect(str(db_path))

    # Drop & recreate so reruns are clean
    con.execute("DROP TABLE IF EXISTS supplier_kpis;")

    # Create KPI table from a join of suppliers + POs + deliveries
    con.execute(
        """
        CREATE TABLE supplier_kpis AS
        WITH joined AS (
            SELECT
                s.supplier_id,
                s.supplier_name,
                s.category,
                s.country,
                s.financial_risk_score,
                po.po_id,
                po.order_date,
                po.promised_date,
                po.quantity_ordered,
                d.delivery_date,
                d.quantity_delivered,
                d.quality_issues,
                -- delivery delay in days (can be negative)
                DATE_DIFF('day', po.promised_date, d.delivery_date) AS delivery_delay_days,
                -- on-time flag (1 if on-time or early, else 0)
                CASE WHEN d.delivery_date <= po.promised_date THEN 1 ELSE 0 END AS on_time_flag
            FROM suppliers s
            JOIN purchase_orders po
                ON s.supplier_id = po.supplier_id
            JOIN deliveries d
                ON po.po_id = d.po_id
        )
        SELECT
            supplier_id,
            supplier_name,
            category,
            country,
            financial_risk_score,

            AVG(on_time_flag)::DOUBLE AS on_time_delivery_rate,
            AVG(delivery_delay_days)::DOUBLE AS avg_delivery_delay_days,

            (SUM(quantity_delivered)::DOUBLE / NULLIF(SUM(quantity_ordered), 0)) AS fill_rate,

            AVG(quality_issues)::DOUBLE AS quality_issue_rate,

            COUNT(*) AS n_pos
        FROM joined
        GROUP BY
            supplier_id, supplier_name, category, country, financial_risk_score
        ORDER BY supplier_id;
        """
    )

    # Quick sanity peek: top 5 worst on-time, and top 5 highest delays
    print("\nTop 5 lowest on-time delivery rates:")
    print(
        con.execute(
            """
            SELECT supplier_id, supplier_name, on_time_delivery_rate, n_pos
            FROM supplier_kpis
            ORDER BY on_time_delivery_rate ASC
            LIMIT 5;
            """
        ).fetchdf()
    )

    print("\nTop 5 highest average delivery delays:")
    print(
        con.execute(
            """
            SELECT supplier_id, supplier_name, avg_delivery_delay_days, n_pos
            FROM supplier_kpis
            ORDER BY avg_delivery_delay_days DESC
            LIMIT 5;
            """
        ).fetchdf()
    )

    con.close()
    print("\nCreated table: supplier_kpis")


if __name__ == "__main__":
    main()
