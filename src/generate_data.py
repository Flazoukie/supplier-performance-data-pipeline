from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from random import Random
import csv


@dataclass
class Config:
    seed: int = 42
    n_suppliers: int = 15
    n_pos: int = 600  # total purchase orders
    start_date: date = date(2024, 1, 1)
    end_date: date = date(2024, 12, 31)

    # Lead time (promised_date - order_date)
    lead_time_min_days: int = 3
    lead_time_max_days: int = 21

    # Delivery lateness (delivery_date - promised_date)
    # We'll make most on time, some late, a few early.
    late_prob: float = 0.22
    early_prob: float = 0.08
    late_min_days: int = 1
    late_max_days: int = 14
    early_min_days: int = 1
    early_max_days: int = 4

    # Quantity & quality
    qty_min: int = 10
    qty_max: int = 500
    partial_delivery_prob: float = 0.18
    partial_min_ratio: float = 0.6
    partial_max_ratio: float = 0.95
    base_quality_issue_prob: float = 0.04  # baseline probability


CATEGORIES = ["Packaging", "Raw Materials", "Logistics", "Electronics", "Textiles"]
COUNTRIES = ["DE", "PL", "CZ", "NL", "IT", "ES", "FR", "TR", "CN"]


def rand_date(rng: Random, start: date, end: date) -> date:
    """Uniform random date between start and end (inclusive)."""
    span = (end - start).days
    return start + timedelta(days=rng.randint(0, span))


def clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))


def main() -> None:
    cfg = Config()
    rng = Random(cfg.seed)

    project_root = Path(__file__).resolve().parents[1]
    data_dir = project_root / "data"
    data_dir.mkdir(parents=True, exist_ok=True)

    suppliers_path = data_dir / "suppliers.csv"
    pos_path = data_dir / "purchase_orders.csv"
    deliveries_path = data_dir / "deliveries.csv"

    # --- Suppliers ---
    suppliers = []
    for i in range(1, cfg.n_suppliers + 1):
        supplier_id = f"S{i:03d}"
        supplier = {
            "supplier_id": supplier_id,
            "supplier_name": f"Supplier {i:02d}",
            "category": rng.choice(CATEGORIES),
            "country": rng.choice(COUNTRIES),
            "financial_risk_score": rng.randint(0, 100),
        }
        suppliers.append(supplier)

    # A simple “supplier behavior” profile:
    # Worse financial risk -> slightly higher late probability and quality issues.
    supplier_profile = {}
    for s in suppliers:
        fin = s["financial_risk_score"] / 100.0
        # Increase late probability up to +0.25 and quality issues up to +0.06
        late_p = clamp(cfg.late_prob + 0.25 * fin, 0.05, 0.65)
        q_p = clamp(cfg.base_quality_issue_prob + 0.06 * fin, 0.01, 0.20)
        partial_p = clamp(cfg.partial_delivery_prob + 0.15 * fin, 0.05, 0.55)
        supplier_profile[s["supplier_id"]] = {
            "late_prob": late_p,
            "quality_prob": q_p,
            "partial_prob": partial_p,
        }

    # --- Purchase Orders ---
    purchase_orders = []
    for j in range(1, cfg.n_pos + 1):
        po_id = f"PO{j:05d}"
        supplier_id = rng.choice(suppliers)["supplier_id"]
        order_date = rand_date(rng, cfg.start_date, cfg.end_date)

        lead_time = rng.randint(cfg.lead_time_min_days, cfg.lead_time_max_days)
        promised_date = order_date + timedelta(days=lead_time)

        qty = rng.randint(cfg.qty_min, cfg.qty_max)

        purchase_orders.append(
            {
                "po_id": po_id,
                "supplier_id": supplier_id,
                "order_date": order_date.isoformat(),
                "promised_date": promised_date.isoformat(),
                "quantity_ordered": qty,
            }
        )

    # --- Deliveries (1 per PO in this minimal scope) ---
    deliveries = []
    for po in purchase_orders:
        supplier_id = po["supplier_id"]
        prof = supplier_profile[supplier_id]

        promised = date.fromisoformat(po["promised_date"])

        # Decide early / late / on-time
        r = rng.random()
        if r < prof["late_prob"]:
            delay = rng.randint(cfg.late_min_days, cfg.late_max_days)
            delivery_date = promised + timedelta(days=delay)
        elif r < prof["late_prob"] + cfg.early_prob:
            early = rng.randint(cfg.early_min_days, cfg.early_max_days)
            delivery_date = promised - timedelta(days=early)
        else:
            # on promised date
            delivery_date = promised

        ordered = int(po["quantity_ordered"])

        # Partial deliveries
        if rng.random() < prof["partial_prob"]:
            ratio = rng.uniform(cfg.partial_min_ratio, cfg.partial_max_ratio)
            delivered_qty = max(0, int(round(ordered * ratio)))
        else:
            delivered_qty = ordered

        # Quality issues
        quality_issues = 1 if rng.random() < prof["quality_prob"] else 0

        deliveries.append(
            {
                "po_id": po["po_id"],
                "delivery_date": delivery_date.isoformat(),
                "quantity_delivered": delivered_qty,
                "quality_issues": quality_issues,
            }
        )

    # --- Write CSVs ---
    def write_csv(path: Path, rows: list[dict], fieldnames: list[str]) -> None:
        with path.open("w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=fieldnames)
            w.writeheader()
            w.writerows(rows)

    write_csv(
        suppliers_path,
        suppliers,
        ["supplier_id", "supplier_name", "category", "country", "financial_risk_score"],
    )
    write_csv(
        pos_path,
        purchase_orders,
        ["po_id", "supplier_id", "order_date", "promised_date", "quantity_ordered"],
    )
    write_csv(
        deliveries_path,
        deliveries,
        ["po_id", "delivery_date", "quantity_delivered", "quality_issues"],
    )

    print("Generated:")
    print(f"- {suppliers_path}")
    print(f"- {pos_path}")
    print(f"- {deliveries_path}")


if __name__ == "__main__":
    main()
