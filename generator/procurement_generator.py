"""
Synthetic procure-to-pay (P2P) data generator.

Simulates a procurement system (think Coupa or SAP Ariba) dropping one extract per
batch into a Unity Catalog volume:

    <base_path>/<entity>/batch=<n>/<entity>_batch<nnn>.<csv|json>

Each batch covers a DAYS_PER_BATCH window starting at start_date, so five batches give
five months of activity for the monthly marts.

Batches must be generated in order (1, 2, 3, ...). Each batch continues from the state
the previous batch left behind (supplier versions, open POs, late receipts), saved as
JSON under <base_path>/_generator_state/. Rerunning a batch with the same parameters
rewrites identical files.

Deliberate problems, so the silver layer has something to fix:
  - hard defects (error_rate of rows): nulls in required fields, bad dates, negative
    quantities, invalid statuses, unknown keys. These should go to quarantine.
  - soft defects (error_rate of rows): inconsistent casing and whitespace. These should
    be standardised in silver, not rejected.
  - duplicates: exact copies from the previous batch and within the same batch.
  - updates: suppliers and POs re-sent with a newer updated_at.
  - late-arriving goods receipts: sent one batch after the batch they belong to.
  - maverick spend: invoices with no PO.
  - price variances: invoice amount does not match received qty x unit price.
  - schema evolution: purchase_orders gain a contract_id column from batch 5.
"""

from __future__ import annotations

import copy
import json
import csv
import random
import shutil
from collections import Counter
from datetime import date, datetime, timedelta
from pathlib import Path

# ---------------------------------------------------------------------------
# Simulation settings
# ---------------------------------------------------------------------------

ENTITIES = ("suppliers", "purchase_orders", "goods_receipts", "invoices")

DAYS_PER_BATCH = 30
INITIAL_SUPPLIERS = 40
NEW_SUPPLIERS_PER_BATCH = 3
SUPPLIER_CHANGE_RATE = 0.15      # share of suppliers that change terms or status per batch
CANCEL_RATE = 0.03               # POs cancelled before delivery
ON_TIME_RATE = 0.75              # PO lines delivered on or before the expected date
SHORT_DELIVERY_RATE = 0.12       # PO lines delivered short (not in full)
PRICE_VARIANCE_RATE = 0.10       # invoices that do not match PO price x received qty
LATE_ARRIVAL_RATE = 0.10         # receipts that reach the system one batch late
MAVERICK_RATE = 0.08             # non-PO invoices, relative to rows_per_entity
UNPAID_RATE = 0.08
LATE_PAYMENT_RATE = 0.22
DUPLICATE_POOL_SIZE = 200        # rows kept per entity to re-send as duplicates next batch
SCHEMA_EVOLUTION_BATCH = 5       # purchase_orders gain contract_id from this batch

# ---------------------------------------------------------------------------
# Reference data
# ---------------------------------------------------------------------------

CATEGORIES = {
    "Technology": {
        "items": ["Laptop", "Monitor", "Cloud credits", "Software licence", "Docking station"],
        "min_price": 40, "max_price": 2500, "name_word": "Systems",
    },
    "Marketing": {
        "items": ["TV airtime", "Social media campaign", "Sponsorship package", "Print advertising"],
        "min_price": 500, "max_price": 20000, "name_word": "Media",
    },
    "Professional Services": {
        "items": ["Consulting day", "Legal advice hour", "Audit fee", "Recruitment fee"],
        "min_price": 150, "max_price": 1800, "name_word": "Partners",
    },
    "Facilities": {
        "items": ["Office cleaning", "Desk chair", "Catering", "Security service"],
        "min_price": 20, "max_price": 900, "name_word": "Facilities",
    },
    "Telecoms": {
        "items": ["Mobile plan", "Leased line", "SIM card", "Conference bridge"],
        "min_price": 10, "max_price": 1200, "name_word": "Telecom",
    },
    "Data & Content": {
        "items": ["Sports data feed", "Odds data licence", "Streaming rights", "Market data subscription"],
        "min_price": 1000, "max_price": 30000, "name_word": "Data",
    },
    "Travel": {
        "items": ["Flight", "Hotel night", "Rail ticket", "Car hire"],
        "min_price": 60, "max_price": 700, "name_word": "Travel",
    },
}

# country code -> legal suffix used in supplier names
COUNTRIES = {"GB": "Ltd", "IE": "Ltd", "RO": "SRL", "MT": "Ltd", "US": "Inc", "DE": "GmbH", "ES": "SL"}

NAME_PREFIXES = [
    "Atlas", "Carpathian", "Danube", "Meridian", "Orion", "Summit", "Keystone", "Granite",
    "Blue Harbour", "Northwind", "Vertex", "Lumen", "Copperfield", "Riverside", "Silverline",
]

PAYMENT_TERMS = [15, 30, 45, 60, 90]
SUPPLIER_STATUSES = ["ACTIVE", "INACTIVE", "BLOCKED"]
PO_STATUSES = ["APPROVED", "PARTIALLY_RECEIVED", "RECEIVED", "CLOSED", "CANCELLED"]

# buyer -> cost centre (each buyer belongs to one cost centre, used by the row filter later)
BUYERS = {
    "ana.popescu": "CC-100",
    "ioana.rusu": "CC-100",
    "mihai.ionescu": "CC-200",
    "sarah.oconnor": "CC-200",
    "tom.walsh": "CC-300",
    "elena.marin": "CC-400",
    "david.chen": "CC-500",
}

BAD_DATES = ["2026-02-30", "31/01/2026", "N/A", "2026-13-01"]
BAD_TIMESTAMPS = ["2026-02-30T10:00:00", "yesterday", "2026/01/15 25:61:00"]
UNKNOWN_SUPPLIER_ID = "SUP-9999"

CSV_COLUMNS = {
    "suppliers": ["supplier_id", "name", "country", "category", "payment_terms_days", "status", "updated_at"],
    "goods_receipts": ["receipt_id", "po_id", "po_line_id", "received_qty", "received_date"],
}

# ---------------------------------------------------------------------------
# Small helpers
# ---------------------------------------------------------------------------


def batch_window(start: date, batch_number: int) -> tuple[date, date]:
    """First and last day covered by a batch."""
    first = start + timedelta(days=(batch_number - 1) * DAYS_PER_BATCH)
    return first, first + timedelta(days=DAYS_PER_BATCH - 1)


def random_date(rng: random.Random, first: date, last: date) -> date:
    return first + timedelta(days=rng.randint(0, max(0, (last - first).days)))


def random_timestamp(rng: random.Random, day: date) -> str:
    """A random office-hours time on `day`, as an ISO timestamp string."""
    seconds = rng.randint(7 * 3600, 19 * 3600)
    return (datetime.combine(day, datetime.min.time()) + timedelta(seconds=seconds)).isoformat()


def next_id(state: dict, kind: str, prefix: str, width: int = 6) -> str:
    state["seq"][kind] += 1
    return f"{prefix}-{state['seq'][kind]:0{width}d}"


def messy_text(rng: random.Random, value: str) -> str:
    """Same value with the casing and whitespace problems real extracts have."""
    return rng.choice([value.lower(), value.title(), f" {value} ", f"{value.lower()} "])




def write_jsonl(rows: list[dict], path: Path) -> None:
    with open(path, "w", encoding="utf-8") as f:
        for row in rows:
            my_row = json.dumps(row, ensure_ascii=False)
            f.write(my_row + "\n")




def write_csv(rows: list[dict], path: Path, columns: list[str]) -> None:
    with open(path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f,fieldnames=columns)
        writer.writeheader()
        writer.writerows(rows)





# ---------------------------------------------------------------------------
# Suppliers
# ---------------------------------------------------------------------------


def make_supplier(rng: random.Random, state: dict, updated_on: date) -> dict:
    category = rng.choice(list(CATEGORIES))
    country = rng.choice(list(COUNTRIES))
    name = f"{rng.choice(NAME_PREFIXES)} {CATEGORIES[category]['name_word']} {COUNTRIES[country]}"
    return {
        "supplier_id": next_id(state, "supplier", "SUP", width=4),
        "name": name,
        "country": country,
        "category": category,
        "payment_terms_days": rng.choice(PAYMENT_TERMS),
        "status": "ACTIVE",
        "updated_at": random_timestamp(rng, updated_on),
    }


def change_supplier(rng: random.Random, supplier: dict, updated_on: date) -> dict:
    """A new version of the supplier with different payment terms or a different status."""
    new = dict(supplier)
    if rng.random() < 0.6:
        new["payment_terms_days"] = rng.choice([t for t in PAYMENT_TERMS if t != supplier["payment_terms_days"]])
    else:
        new["status"] = rng.choice([s for s in SUPPLIER_STATUSES if s != supplier["status"]])
    new["updated_at"] = random_timestamp(rng, updated_on)
    return new


def update_suppliers(rng: random.Random, state: dict, batch_number: int, first: date, last: date) -> list[dict]:
    """Onboard and change suppliers. Returns the supplier rows this batch's extract contains."""
    if batch_number == 1:
        # Initial versions predate the first batch, so every PO can find a valid supplier version.
        new = [make_supplier(rng, state, first - timedelta(days=rng.randint(30, 365)))
               for _ in range(INITIAL_SUPPLIERS)]
        changed = []
    else:
        # New suppliers are onboarded the day before the window, for the same reason.
        new = [make_supplier(rng, state, first - timedelta(days=1)) for _ in range(NEW_SUPPLIERS_PER_BATCH)]
        existing = sorted(state["suppliers"])
        to_change = rng.sample(existing, round(len(existing) * SUPPLIER_CHANGE_RATE))
        changed = [change_supplier(rng, state["suppliers"][sid], random_date(rng, first, last))
                   for sid in to_change]

    for supplier in new + changed:
        state["suppliers"][supplier["supplier_id"]] = supplier
    return new + changed


# ---------------------------------------------------------------------------
# Purchase orders and their lifecycle
# ---------------------------------------------------------------------------


def plan_lifecycle(rng: random.Random, order_day: date, lines: list[dict]) -> dict:
    """
    Decide up front what will happen to a PO: cancellation, or a receipt per line
    followed by an invoice. Each batch then emits the events dated inside its window.
    """
    if rng.random() < CANCEL_RATE:
        return {"cancel_on": (order_day + timedelta(days=rng.randint(1, 10))).isoformat()}

    receipts = {}
    for line in lines:
        expected = date.fromisoformat(line["expected_delivery_date"])
        if rng.random() < ON_TIME_RATE:
            received_on = expected - timedelta(days=rng.randint(0, 3))
        else:
            received_on = expected + timedelta(days=rng.randint(1, 14))
        qty = line["quantity"]
        if rng.random() < SHORT_DELIVERY_RATE:
            qty = max(1, int(qty * rng.uniform(0.5, 0.9)))
        receipts[line["po_line_id"]] = {"received_on": received_on.isoformat(), "received_qty": qty}

    last_receipt = max(date.fromisoformat(r["received_on"]) for r in receipts.values())
    variance = rng.uniform(1.02, 1.20) if rng.random() < PRICE_VARIANCE_RATE else 1.0
    return {
        "receipts": receipts,
        "invoice_on": (last_receipt + timedelta(days=rng.randint(0, 10))).isoformat(),
        "invoice_variance": round(variance, 4),
    }


def make_purchase_order(rng: random.Random, state: dict, batch_number: int, supplier: dict,
                        first: date, last: date) -> dict:
    po_id = next_id(state, "po", "PO")
    order_day = random_date(rng, first, last)
    buyer = rng.choice(list(BUYERS))
    category = CATEGORIES[supplier["category"]]

    lines = []
    for n in range(1, rng.randint(1, 4) + 1):
        lines.append({
            "po_line_id": f"{po_id}-{n}",
            "item": rng.choice(category["items"]),
            "category": supplier["category"],
            "quantity": rng.randint(1, 50),
            "unit_price": round(rng.uniform(category["min_price"], category["max_price"]), 2),
            "expected_delivery_date": (order_day + timedelta(days=rng.randint(3, 21))).isoformat(),
        })

    po = {
        "po_id": po_id,
        "supplier_id": supplier["supplier_id"],
        "buyer": buyer,
        "cost_centre": BUYERS[buyer],
        "order_date": order_day.isoformat(),
        "status": "APPROVED",
        "updated_at": random_timestamp(rng, order_day),
        "lines": lines,
    }
    if batch_number >= SCHEMA_EVOLUTION_BATCH:
        po["contract_id"] = f"CTR-{rng.randint(1, 60):04d}" if rng.random() < 0.7 else None
    return po


def create_purchase_orders(rng: random.Random, state: dict, batch_number: int, count: int,
                           first: date, last: date) -> list[dict]:
    """Create new POs, register them as open, and return snapshots of their first version."""
    active = [s for s in state["suppliers"].values() if s["status"] == "ACTIVE"]
    candidates = active or list(state["suppliers"].values())
    snapshots = []
    for _ in range(count):
        po = make_purchase_order(rng, state, batch_number, rng.choice(candidates), first, last)
        plan = plan_lifecycle(rng, date.fromisoformat(po["order_date"]), po["lines"])
        state["open_pos"][po["po_id"]] = {"po": po, "plan": plan, "received": []}
        snapshots.append(copy.deepcopy(po))
    return snapshots


def new_po_version(rng: random.Random, po: dict, status: str, on: date) -> dict:
    """Move a PO to a new status. updated_at always increases, even within the same day."""
    updated_at = random_timestamp(rng, on)
    previous = datetime.fromisoformat(po["updated_at"])
    if datetime.fromisoformat(updated_at) <= previous:
        updated_at = (previous + timedelta(minutes=1)).isoformat()
    po["status"] = status
    po["updated_at"] = updated_at
    return copy.deepcopy(po)


def payment_date(rng: random.Random, invoice_on: date, due_on: date) -> str | None:
    # Simplification: the payment outcome is known when the invoice is generated.
    roll = rng.random()
    if roll < UNPAID_RATE:
        return None
    if roll < UNPAID_RATE + LATE_PAYMENT_RATE:
        return (due_on + timedelta(days=rng.randint(1, 40))).isoformat()
    return random_date(rng, invoice_on, due_on).isoformat()


def make_invoice(rng: random.Random, state: dict, supplier_id: str, po_id: str | None,
                 invoice_on: date, amount: float) -> dict:
    terms = state["suppliers"][supplier_id]["payment_terms_days"]
    due_on = invoice_on + timedelta(days=terms)
    return {
        "invoice_id": next_id(state, "invoice", "INV"),
        "supplier_id": supplier_id,
        "po_id": po_id,
        "invoice_date": invoice_on.isoformat(),
        "amount": round(amount, 2),
        "due_date": due_on.isoformat(),
        "paid_date": payment_date(rng, invoice_on, due_on),
    }


def progress_open_pos(rng: random.Random, state: dict, first: date, last: date):
    """
    Emit the events of every open PO that fall inside this window:
    cancellations, goods receipts, status updates and invoices.
    Returns (po_updates, receipts_now, receipts_held_back, invoices).
    """
    po_updates, receipts_now, receipts_late, invoices = [], [], [], []

    for po_id in list(state["open_pos"]):
        entry = state["open_pos"][po_id]
        po, plan = entry["po"], entry["plan"]

        if "cancel_on" in plan:
            if first <= date.fromisoformat(plan["cancel_on"]) <= last:
                po_updates.append(new_po_version(rng, po, "CANCELLED", date.fromisoformat(plan["cancel_on"])))
                del state["open_pos"][po_id]
            continue

        received_now = []
        for line_id, planned in plan["receipts"].items():
            received_on = date.fromisoformat(planned["received_on"])
            if line_id in entry["received"] or not first <= received_on <= last:
                continue
            receipt = {
                "receipt_id": next_id(state, "receipt", "GR"),
                "po_id": po_id,
                "po_line_id": line_id,
                "received_qty": planned["received_qty"],
                "received_date": planned["received_on"],
            }
            (receipts_late if rng.random() < LATE_ARRIVAL_RATE else receipts_now).append(receipt)
            entry["received"].append(line_id)
            received_now.append(received_on)

        if received_now:
            fully_received = len(entry["received"]) == len(plan["receipts"])
            status = "RECEIVED" if fully_received else "PARTIALLY_RECEIVED"
            po_updates.append(new_po_version(rng, po, status, max(received_now)))

        invoice_on = date.fromisoformat(plan["invoice_on"])
        if first <= invoice_on <= last:
            # invoice_on is after every receipt date, so all lines are received by now
            prices = {line["po_line_id"]: line["unit_price"] for line in po["lines"]}
            amount = sum(r["received_qty"] * prices[lid] for lid, r in plan["receipts"].items())
            invoices.append(make_invoice(rng, state, po["supplier_id"], po_id, invoice_on,
                                         amount * plan["invoice_variance"]))
            po_updates.append(new_po_version(rng, po, "CLOSED", invoice_on))
            del state["open_pos"][po_id]

    return po_updates, receipts_now, receipts_late, invoices


def make_maverick_invoices(rng: random.Random, state: dict, rows_per_entity: int,
                           first: date, last: date) -> list[dict]:
    """Invoices with no PO: spend that bypassed the procurement process."""
    active = [s for s in state["suppliers"].values() if s["status"] == "ACTIVE"]
    candidates = active or list(state["suppliers"].values())
    invoices = []
    for _ in range(max(1, round(rows_per_entity * MAVERICK_RATE))):
        supplier = rng.choice(candidates)
        category = CATEGORIES[supplier["category"]]
        amount = rng.uniform(category["min_price"], category["max_price"]) * rng.randint(1, 5)
        invoices.append(make_invoice(rng, state, supplier["supplier_id"], None,
                                     random_date(rng, first, last), amount))
    return invoices


def to_po_row(po: dict, batch_number: int) -> dict:
    """The PO as it appears in the extract. From batch 5 every row carries contract_id."""
    row = copy.deepcopy(po)
    if batch_number >= SCHEMA_EVOLUTION_BATCH:
        lines = row.pop("lines")
        row["contract_id"] = row.get("contract_id")
        row["lines"] = lines
    else:
        row.pop("contract_id", None)
    return row


# ---------------------------------------------------------------------------
# Data quality problems
# ---------------------------------------------------------------------------


def _negate_first_line_quantity(rng, row):
    row["lines"][0]["quantity"] = -abs(row["lines"][0]["quantity"])


def _null_first_line_price(rng, row):
    row["lines"][0]["unit_price"] = None


def _due_before_invoice(rng, row):
    invoice_on = date.fromisoformat(row["invoice_date"])
    row["due_date"] = (invoice_on - timedelta(days=rng.randint(1, 30))).isoformat()


# Hard defects: the row is wrong and should be quarantined in silver.
HARD_DEFECTS = {
    "suppliers": {
        "null_supplier_id": lambda rng, r: r.update(supplier_id=None),
        "null_name": lambda rng, r: r.update(name=None),
        "invalid_status": lambda rng, r: r.update(status=rng.choice(["ACTV", "UNKNOWN", "PENDING"])),
        "negative_payment_terms": lambda rng, r: r.update(payment_terms_days=-r["payment_terms_days"]),
        "bad_updated_at": lambda rng, r: r.update(updated_at=rng.choice(BAD_TIMESTAMPS)),
    },
    "purchase_orders": {
        "null_supplier_id": lambda rng, r: r.update(supplier_id=None),
        "unknown_supplier_id": lambda rng, r: r.update(supplier_id=UNKNOWN_SUPPLIER_ID),
        "invalid_status": lambda rng, r: r.update(status=rng.choice(["APPRVD", "OPEN?", "DRAFT"])),
        "bad_order_date": lambda rng, r: r.update(order_date=rng.choice(BAD_DATES)),
        "negative_quantity": _negate_first_line_quantity,
        "null_unit_price": _null_first_line_price,
        "no_lines": lambda rng, r: r.update(lines=[]),
    },
    "goods_receipts": {
        "negative_received_qty": lambda rng, r: r.update(received_qty=-abs(r["received_qty"])),
        "bad_received_date": lambda rng, r: r.update(received_date=rng.choice(BAD_DATES)),
        "null_po_id": lambda rng, r: r.update(po_id=None),
        "unknown_po_line_id": lambda rng, r: r.update(po_line_id=f"{r['po_id']}-99"),
    },
    "invoices": {
        "negative_amount": lambda rng, r: r.update(amount=-abs(r["amount"])),
        "amount_as_text": lambda rng, r: r.update(amount=f"{r['amount']:,.2f}"),
        "null_invoice_date": lambda rng, r: r.update(invoice_date=None),
        "due_before_invoice": _due_before_invoice,
        "unknown_supplier_id": lambda rng, r: r.update(supplier_id=UNKNOWN_SUPPLIER_ID),
    },
}

# Soft defects: the value is right but formatted inconsistently. Silver should fix these.
SOFT_DEFECTS = {
    "suppliers": {
        "messy_status": lambda rng, r: r.update(status=messy_text(rng, r["status"])),
        "messy_country": lambda rng, r: r.update(country=r["country"].lower()),
        "messy_name": lambda rng, r: r.update(name=f"  {r['name']} "),
    },
    "purchase_orders": {
        "messy_status": lambda rng, r: r.update(status=messy_text(rng, r["status"])),
        "messy_cost_centre": lambda rng, r: r.update(cost_centre=r["cost_centre"].lower()),
    },
}


def inject_problems(rng: random.Random, rows: list[dict], error_rate: float,
                    hard: dict, soft: dict) -> Counter:
    """Corrupt error_rate of the rows with one hard defect each, and as many with a soft defect."""
    counts = Counter()
    n = round(len(rows) * error_rate)
    for defects, label in ((hard, "hard"), (soft, "soft")):
        if not defects:
            continue
        for i in rng.sample(range(len(rows)), n):
            name = rng.choice(sorted(defects))
            defects[name](rng, rows[i])
            counts[f"{label}:{name}"] += 1
    return counts


def add_duplicates(rng: random.Random, rows: list[dict], previous_rows: list[dict],
                   error_rate: float) -> tuple[list[dict], int, int]:
    """Append exact copies of rows from the previous batch and from this batch, then shuffle."""
    n = round(len(rows) * error_rate)
    cross_batch = rng.sample(previous_rows, min(n, len(previous_rows)))
    in_batch = [rng.choice(rows) for _ in range(n // 2)] if rows else []
    out = rows + copy.deepcopy(cross_batch) + copy.deepcopy(in_batch)
    rng.shuffle(out)
    return out, len(cross_batch), len(in_batch)


# ---------------------------------------------------------------------------
# State and output
# ---------------------------------------------------------------------------


def state_path(base: Path, batch_number: int) -> Path:
    # Folders starting with "_" are ignored by Spark and Auto Loader file listing.
    return base / "_generator_state" / f"state_after_batch_{batch_number:03d}.json"


def load_state(base: Path, batch_number: int, start_date: str) -> dict:
    if batch_number == 1:
        return {
            "start_date": start_date,
            "seq": {"supplier": 0, "po": 0, "receipt": 0, "invoice": 0},
            "suppliers": {},
            "open_pos": {},
            "pending_receipts": [],
            "recent_rows": {entity: [] for entity in ENTITIES},
        }
    path = state_path(base, batch_number - 1)
    if not path.exists():
        raise FileNotFoundError(f"Generate batch {batch_number - 1} first ({path} not found)")
    return json.loads(path.read_text(encoding="utf-8"))


def save_state(base: Path, batch_number: int, state: dict) -> None:
    path = state_path(base, batch_number)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(state), encoding="utf-8")


def write_entity(base: Path, entity: str, batch_number: int, rows: list[dict]) -> Path:
    """Replace the batch folder for one entity, so a rerun leaves exactly one file."""
    folder = base / entity / f"batch={batch_number}"
    if folder.exists():
        shutil.rmtree(folder)
    folder.mkdir(parents=True)
    if entity in CSV_COLUMNS:
        path = folder / f"{entity}_batch{batch_number:03d}.csv"
        write_csv(rows, path, CSV_COLUMNS[entity])
    else:
        path = folder / f"{entity}_batch{batch_number:03d}.json"
        write_jsonl(rows, path)
    return path


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def generate_batch(batch_number: int, start_date: str = "2026-01-01", rows_per_entity: int = 200,
                   error_rate: float = 0.05, seed: int = 42,
                   base_path: str = "/Volumes/procurement/landing/raw") -> dict:
    """
    Generate one batch of extracts and return a summary of what was written.

    rows_per_entity is the number of new purchase orders per batch. Receipts and
    invoices follow from the PO lifecycle, so their counts are roughly proportional.
    start_date is only read for batch 1; later batches continue from the saved state.
    """
    if batch_number < 1:
        raise ValueError("batch_number must be 1 or more")
    if not 0 <= error_rate <= 0.5:
        raise ValueError("error_rate must be between 0 and 0.5")

    base = Path(base_path)
    state = load_state(base, batch_number, start_date)
    rng = random.Random(f"{seed}-{batch_number}")
    first, last = batch_window(date.fromisoformat(state["start_date"]), batch_number)

    supplier_rows = update_suppliers(rng, state, batch_number, first, last)
    new_pos = create_purchase_orders(rng, state, batch_number, rows_per_entity, first, last)
    late_from_previous = state["pending_receipts"]
    po_updates, receipts_now, receipts_late, po_invoices = progress_open_pos(rng, state, first, last)
    maverick = make_maverick_invoices(rng, state, rows_per_entity, first, last)
    state["pending_receipts"] = receipts_late

    clean = {
        "suppliers": supplier_rows,
        "purchase_orders": [to_po_row(po, batch_number) for po in new_pos + po_updates],
        "goods_receipts": late_from_previous + receipts_now,
        "invoices": po_invoices + maverick,
    }

    summary = {"batch_number": batch_number, "window": f"{first} to {last}"}
    for entity, clean_rows in clean.items():
        rows = copy.deepcopy(clean_rows)
        defects = inject_problems(rng, rows, error_rate, HARD_DEFECTS[entity], SOFT_DEFECTS.get(entity, {}))
        rows, cross_batch, in_batch = add_duplicates(rng, rows, state["recent_rows"][entity], error_rate)
        state["recent_rows"][entity] = rng.sample(rows, min(len(rows), DUPLICATE_POOL_SIZE))
        path = write_entity(base, entity, batch_number, rows)
        summary[entity] = {
            "file": str(path),
            "rows_written": len(rows),
            "unique_clean_rows": len(clean_rows),
            "cross_batch_duplicates": cross_batch,
            "in_batch_duplicates": in_batch,
            "defects": dict(sorted(defects.items())),
        }

    summary["purchase_orders"]["new_pos"] = len(new_pos)
    summary["purchase_orders"]["status_updates"] = len(po_updates)
    summary["goods_receipts"]["late_arriving_from_previous_batch"] = len(late_from_previous)
    summary["goods_receipts"]["held_back_for_next_batch"] = len(receipts_late)
    summary["invoices"]["maverick_invoices"] = len(maverick)

    save_state(base, batch_number, state)
    return summary
