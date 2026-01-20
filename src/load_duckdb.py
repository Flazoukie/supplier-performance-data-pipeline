"""This script loads CSV data into a DuckDB database file (warehouse.db) located at the project root.
It creates three tables: suppliers, purchase_orders, and deliveries.
Each table is created with an explicit schema, and data is loaded from the corresponding CSV files in the data/ directory.
It performs basic sanity checks after loading to ensure data integrity."""

# libraries
from __future__ import annotations

from pathlib import Path
import duckdb # DuckDB Python client


def main() -> None:
    # setup paths
    project_root = Path(__file__).resolve().parents[1] # _file_ is src/load_duckdb.py, so go up two levels to project root and gets the absolute path
    data_dir = project_root / "data" # where we stored the csv files
    db_path = project_root / "warehouse.db" # where we will create the duckdb database file

    # CSV paths
    suppliers_csv = data_dir / "suppliers.csv"
    pos_csv = data_dir / "purchase_orders.csv"
    deliveries_csv = data_dir / "deliveries.csv"

    # Basic guardrails: fail early if inputs are missing
    missing = [p for p in [suppliers_csv, pos_csv, deliveries_csv] if not p.exists()]
    if missing:
        raise FileNotFoundError(f"Missing input files: {missing}")

    # Connect to DuckDB database file (created if it doesn't exist)
    con = duckdb.connect(str(db_path))

    # For a clean rerun experience during development:
    # drop tables if they exist so re-loading doesn't duplicate rows
    con.execute("DROP TABLE IF EXISTS suppliers;")
    con.execute("DROP TABLE IF EXISTS purchase_orders;")
    con.execute("DROP TABLE IF EXISTS deliveries;")

    # Create tables with explicit schemas (clear + explainable) - explicitly defining the schema helps avoid issues with automatic type inference.
    con.execute(
        """
        CREATE TABLE suppliers (
            supplier_id VARCHAR PRIMARY KEY,
            supplier_name VARCHAR,
            category VARCHAR,
            country VARCHAR,
            financial_risk_score INTEGER
        );
        """
    )

    con.execute(
        """
        CREATE TABLE purchase_orders (
            po_id VARCHAR PRIMARY KEY,
            supplier_id VARCHAR,
            order_date DATE,
            promised_date DATE,
            quantity_ordered INTEGER
        );
        """
    )

    con.execute(
        """
        CREATE TABLE deliveries (
            po_id VARCHAR PRIMARY KEY,
            delivery_date DATE,
            quantity_delivered INTEGER,
            quality_issues INTEGER
        );
        """
    )

    # Load CSVs into the tables
    # DuckDB can read CSVs directly and cast to the target schema.
    con.execute(
        f"""
        INSERT INTO suppliers
        SELECT * FROM read_csv_auto('{suppliers_csv.as_posix()}', header=True);
        """
    )

    con.execute(
        f"""
        INSERT INTO purchase_orders
        SELECT * FROM read_csv_auto('{pos_csv.as_posix()}', header=True);
        """
    )

    con.execute(
        f"""
        INSERT INTO deliveries
        SELECT * FROM read_csv_auto('{deliveries_csv.as_posix()}', header=True);
        """
    )

    # --- Sanity checks ---
    suppliers_n = con.execute("SELECT COUNT(*) FROM suppliers;").fetchone()[0]
    pos_n = con.execute("SELECT COUNT(*) FROM purchase_orders;").fetchone()[0]
    deliv_n = con.execute("SELECT COUNT(*) FROM deliveries;").fetchone()[0]

    print("Loaded row counts:")
    print(f"- suppliers:        {suppliers_n}")
    print(f"- purchase_orders:  {pos_n}")
    print(f"- deliveries:       {deliv_n}")

    # Check that every PO has exactly one delivery (minimal scope expectation)
    missing_deliveries = con.execute(
        """
        SELECT COUNT(*) 
        FROM purchase_orders po
        LEFT JOIN deliveries d USING (po_id)
        WHERE d.po_id IS NULL;
        """
    ).fetchone()[0]

    extra_deliveries = con.execute(
        """
        SELECT COUNT(*)
        FROM deliveries d
        LEFT JOIN purchase_orders po USING (po_id)
        WHERE po.po_id IS NULL;
        """
    ).fetchone()[0]

    print("Key integrity checks:")
    print(f"- POs without deliveries: {missing_deliveries}")
    print(f"- Deliveries without POs: {extra_deliveries}")

    con.close()
    print(f"\nDuckDB ready: {db_path}")


if __name__ == "__main__":
    main()
