"""This script computes supplier risk scores based on KPIs stored in DuckDB
and creates a summary table supplier_risk_summary with the results.
The risk score combines normalized performance metrics and financial risk scores
to identify high-risk suppliers.

We use min-max normalization for the KPIs to ensure comparability, then compute a composite risk score
as a weighted average of performance and financial risk.
The final risk score is calculated as:
0.7 * (1 - average_normalized_performance) + 0.3 * (financial_risk_score / 100)
The results are stored in DuckDB as supplier_risk_summary for downstream analysis and reporting.

We verify the top 10 highest risk suppliers by printing them to the console.
"""

from __future__ import annotations

from pathlib import Path
import duckdb


def main() -> None:
    project_root = Path(__file__).resolve().parents[1]
    db_path = project_root / "warehouse.db"
    con = duckdb.connect(str(db_path))

    con.execute("DROP TABLE IF EXISTS supplier_risk_summary;")

    # Normalize KPIs to 0..1 so we can average them:
    # - Higher is better for on_time and fill_rate
    # - Lower is better for delay and quality_issue_rate, so we invert those
    #
    # We use min-max normalization across suppliers:
    # (x - min) / (max - min)
    #
    # Defensive: if max == min, treat normalized value as 1.0 (everyone equal).
    con.execute(
        """
        CREATE TABLE supplier_risk_summary AS
        WITH bounds AS (
            SELECT
                MIN(on_time_delivery_rate) AS min_on_time,
                MAX(on_time_delivery_rate) AS max_on_time,

                MIN(avg_delivery_delay_days) AS min_delay,
                MAX(avg_delivery_delay_days) AS max_delay,

                MIN(fill_rate) AS min_fill,
                MAX(fill_rate) AS max_fill,

                MIN(quality_issue_rate) AS min_q,
                MAX(quality_issue_rate) AS max_q
            FROM supplier_kpis
        ),
        norm AS (
            SELECT
                k.*,

                -- Higher is better
                CASE
                    WHEN b.max_on_time = b.min_on_time THEN 1.0
                    ELSE (k.on_time_delivery_rate - b.min_on_time) / (b.max_on_time - b.min_on_time)
                END AS norm_on_time,

                -- Lower delay is better: normalize then invert
                CASE
                    WHEN b.max_delay = b.min_delay THEN 1.0
                    ELSE 1.0 - ((k.avg_delivery_delay_days - b.min_delay) / (b.max_delay - b.min_delay))
                END AS norm_delay,

                -- Higher is better
                CASE
                    WHEN b.max_fill = b.min_fill THEN 1.0
                    ELSE (k.fill_rate - b.min_fill) / (b.max_fill - b.min_fill)
                END AS norm_fill,

                -- Lower is better: normalize then invert
                CASE
                    WHEN b.max_q = b.min_q THEN 1.0
                    ELSE 1.0 - ((k.quality_issue_rate - b.min_q) / (b.max_q - b.min_q))
                END AS norm_quality

            FROM supplier_kpis k
            CROSS JOIN bounds b
        )
        SELECT
            supplier_id,
            supplier_name,
            category,
            country,
            financial_risk_score,

            on_time_delivery_rate,
            avg_delivery_delay_days,
            fill_rate,
            quality_issue_rate,
            n_pos,

            norm_on_time,
            norm_delay,
            norm_fill,
            norm_quality,

            -- Performance is the average of normalized KPI "goodness"
            (norm_on_time + norm_delay + norm_fill + norm_quality) / 4.0 AS performance_score,

            -- Final risk score per spec
            0.7 * (1.0 - ((norm_on_time + norm_delay + norm_fill + norm_quality) / 4.0))
            + 0.3 * (financial_risk_score / 100.0) AS risk_score

        FROM norm
        ORDER BY risk_score DESC;
        """
    )

    print("\nTop 10 highest risk suppliers:")
    print(
        con.execute(
            """
            SELECT
                supplier_id,
                supplier_name,
                risk_score,
                performance_score,
                financial_risk_score,
                on_time_delivery_rate,
                avg_delivery_delay_days,
                fill_rate,
                quality_issue_rate
            FROM supplier_risk_summary
            ORDER BY risk_score DESC
            LIMIT 10;
            """
        ).fetchdf()
    )

    con.close()
    print("\nCreated table: supplier_risk_summary")


if __name__ == "__main__":
    main()
