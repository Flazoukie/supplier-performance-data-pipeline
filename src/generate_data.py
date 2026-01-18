"""This script generates synthetic data for suppliers, purchase orders, and deliveries.
The data is saved as CSV files in the 'data/' directory.

We generate 600 rows of data for purchase orders and deliveries from 15 suppliers and consider 1 year window (year 2024).
We simulate realistic behaviors considering:
- Lead times promised by suppliers
- Delivery lateness (on-time, early, late) - we go for 22% arriving late and 8% early to simulate real-world scenarios
- Partial deliveries - 18& of partial delivery, with a range between 60% and 95% of ordered quantity
- Quality issues - we simulate a baseline defect rate of 4%. 

example of generated purchase order: 
po = {"po_id": "PO00123", "supplier_id": "S007", "order_date": "2024-06-10", "promised_date": "2024-06-22", "quantity_ordered": 240}
"""

# libraries
from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from random import Random
import csv


# configurations
@dataclass
class Config:
    seed: int = 42 # makes it reproducible

    # suppliers purchase orders
    n_suppliers: int = 15 # num of suppliers
    n_pos: int = 600  # total purchase orders

    # observation windown (1 year)
    start_date: date = date(2024, 1, 1)
    end_date: date = date(2024, 12, 31)

    # Lead time (promised_date - order_date) - how long suppliers say it will take to deliver the product
    lead_time_min_days: int = 3
    lead_time_max_days: int = 21

    # Delivery lateness (delivery_date - promised_date)
    # We generate: most on time, some late, a few early. Delay range: 1 to 14 days.
    late_prob: float = 0.22 # 22% are late
    early_prob: float = 0.08 # 8% are early
    late_min_days: int = 1
    late_max_days: int = 14
    early_min_days: int = 1
    early_max_days: int = 4

    # Quantity & quality
    qty_min: int = 10
    qty_max: int = 500
    partial_delivery_prob: float = 0.18 # 18% chance of partial delivery
    partial_min_ratio: float = 0.6 # partial delivery between 60% ...
    partial_max_ratio: float = 0.95 # and 95% of ordered quantity
    base_quality_issue_prob: float = 0.04  # baseline probability - 4% of the devlivered products have defects


# categories and countries
CATEGORIES = ["Packaging", "Raw Materials", "Logistics", "Electronics", "Textiles"]
COUNTRIES = ["DE", "PL", "CZ", "NL", "IT", "ES", "FR", "TR", "CN"]


# generate csv files


def rand_date(rng: Random, start: date, end: date) -> date:
    """Uniform random date between start and end (inclusive)."""
    span = (end - start).days
    return start + timedelta(days=rng.randint(0, span))


def clamp(x: float, lo: float, hi: float) -> float: # forces x to be between lo and hi.
    # We use it. e.g., when increasing probabilities based on risk scores, to avoid the risk of havinga  probablitity higher than 1.
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
    """"Generate purchase orders with order date, promised date, and quantity ordered."""

    purchase_orders = []
    for j in range(1, cfg.n_pos + 1):
        po_id = f"PO{j:05d}" # e.g., po_id = "PO00123"
        supplier_id = rng.choice(suppliers)["supplier_id"] # e.g., supplier_id = "S007"
        order_date = rand_date(rng, cfg.start_date, cfg.end_date) # e.g., order_date = 2024-06-10

        lead_time = rng.randint(cfg.lead_time_min_days, cfg.lead_time_max_days) # e.g., lead_time = 12 days
        promised_date = order_date + timedelta(days=lead_time) # e.g., promised_date = 2024-06-10 + 12 days = 2024-06-22

        qty = rng.randint(cfg.qty_min, cfg.qty_max) # quantity_ordered = 240

        purchase_orders.append(
            {
                "po_id": po_id,
                "supplier_id": supplier_id,
                "order_date": order_date.isoformat(),
                "promised_date": promised_date.isoformat(),
                "quantity_ordered": qty,
            }
        ) # e.g, po = {"po_id": "PO00123", "supplier_id": "S007", "order_date": "2024-06-10", "promised_date": "2024-06-22", "quantity_ordered": 240}



    # --- Deliveries (1 per PO in this minimal scope) ---
    deliveries = []
    for po in purchase_orders:
        supplier_id = po["supplier_id"]
        prof = supplier_profile[supplier_id] # Looks up that supplier’s “behavior profile” dictionary,
        #i.e., probability of delay, probability of partial delivery or probability of delivering defective products

        promised = date.fromisoformat(po["promised_date"])

        # Decide early / late / on-time
        r = rng.random()
        if r < prof["late_prob"]: # if the random number falls under the supplier's late probability, then the product is late
            delay = rng.randint(cfg.late_min_days, cfg.late_max_days) # chose how many days of delay
            delivery_date = promised + timedelta(days=delay)
        elif r < prof["late_prob"] + cfg.early_prob: # The early probability is fixed (0.8) and does not depend on the supplier profile
            # if r falls between the late_prob and late_prob + early_prob, then the product is early.
            early = rng.randint(cfg.early_min_days, cfg.early_max_days) # chose how early
            delivery_date = promised - timedelta(days=early)
        else:
            # on promised date (on time)
            delivery_date = promised

        ordered = int(po["quantity_ordered"])

        # Partial deliveries
        if rng.random() < prof["partial_prob"]: # if the random number falls under the supplier's partial delivery probability,
            # then it's a partial delivery
            ratio = rng.uniform(cfg.partial_min_ratio, cfg.partial_max_ratio) # get a random ratio between 60% and 95%
            delivered_qty = max(0, int(round(ordered * ratio))) # calculate the delivered quantity
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
