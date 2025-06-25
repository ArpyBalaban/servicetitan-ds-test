# 🧠 Practice Task: Invoice Data Extraction

"""📌 Problem Summary
We are given:
- A pickled file `invoices_new.pkl` containing invoice data (including nested invoice items).
- A text file `expired_invoices.txt` listing expired invoice IDs.

### 🎯 Goal
Write a `DataExtractor` class that:
1. Loads both files
2. Flattens nested invoice data into a pandas DataFrame with specific columns and data types
3. Applies business logic (e.g., type conversion, percentage calc, expired flag)
4. Sorts the output by invoice_id and invoiceitem_id
5. Saves the final DataFrame to CSV"""
"""
---

## 📐 Final Output Columns and Types
| Column                | Type           | Description |
|----------------------|----------------|-------------|
| invoice_id           | str            | ID of invoice (preserve exact string from data) |
| created_on           | datetime64[ns] | Creation date of invoice |
| invoiceitem_id       | int            | ID of the item in invoice |
| invoiceitem_name     | str            | Name of the item |
| type                 | str            | From {0: 'Material', 1: 'Equipment', 2: 'Service', 3: 'Other'} |
| unit_price           | int            | Unit price of the item |
| total_price          | int            | unit_price * quantity |
| percentage_in_invoice| float          | total_price / invoice_total |
| is_expired           | bool           | From expired_invoices.txt |

---

## 🧰 Plan of Attack

1. **Load the data**
    - Load `invoices_new.pkl` using `pickle.load()`
    - Load `expired_invoices.txt` and store IDs in a Python set

```python"""
import pickle
import pandas as pd
from datetime import datetime

class DataExtractor:
    def __init__(self, invoice_path: str, expired_path: str):
        self.invoice_path = invoice_path
        self.expired_path = expired_path
        self.invoices = []
        self.expired_ids = set()

    def load(self):
        # Load invoices
        with open(self.invoice_path, "rb") as f:
            self.invoices = pickle.load(f)

        # Load expired invoice IDs (comma-separated)
        with open(self.expired_path, "r") as f:
            content = f.read()
            self.expired_ids = set(x.strip() for x in content.split(",") if x.strip())

        print(f"Loaded {len(self.invoices)} invoices.")
        print(f"Loaded {len(self.expired_ids)} expired invoice IDs.")

    def extract(self) -> pd.DataFrame:
        type_map = {0: 'Material', 1: 'Equipment', 2: 'Service', 3: 'Other'}
        rows = []

        for invoice in self.invoices:
            invoice_id = invoice["id"]
            try:
                created_on = pd.to_datetime(invoice["created_on"], errors="raise")
            except Exception as e:
                print(f"Skipping invoice {invoice_id} due to invalid date: {invoice['created_on']} ({e})")
                continue

            is_expired = invoice_id in self.expired_ids

            item_rows = []
            invoice_total = 0

            if "items" not in invoice or not isinstance(invoice["items"], list):
                print(f"Skipping invoice {invoice_id}: no items found.")
                continue

            for entry in invoice["items"]:
                item = entry.get("item", {})
                try:
                    unit_price = int(float(item["unit_price"]))
                    quantity = int(float(entry["quantity"]))

                    # Sanity check for quantity
                    if quantity <= 0 or quantity > 10000:
                        print(f"Skipping item with suspicious quantity={quantity} (invoice {invoice_id}, item {item.get('id')})")
                        continue

                    total_price = unit_price * quantity
                    invoice_total += total_price

                    item_rows.append({
                        "invoice_id": invoice_id,
                        "created_on": created_on,
                        "invoiceitem_id": item["id"],
                        "invoiceitem_name": item["name"],
                        "type": type_map.get(item["type"], "Other"),
                        "unit_price": unit_price,
                        "total_price": total_price,
                        "quantity": quantity,
                        "is_expired": is_expired
                    })

                except (ValueError, TypeError, KeyError) as e:
                    print(f"Skipping item due to invalid data (invoice {invoice_id}, item {item.get('id')}): {e}")
                    continue

            for row in item_rows:
                row["percentage_in_invoice"] = row["total_price"] / invoice_total if invoice_total else 0.0
                del row["quantity"]  # drop helper field
                rows.append(row)

        df = pd.DataFrame(rows)
        df = df.astype({
            "invoice_id": str,
            "invoiceitem_id": int,
            "invoiceitem_name": str,
            "type": str,
            "unit_price": int,
            "total_price": int,
            "percentage_in_invoice": float,
            "is_expired": bool
        })
        df = df.sort_values(by=["invoice_id", "invoiceitem_id"]).reset_index(drop=True)
        return df
if __name__ == "__main__":
    extractor = DataExtractor("invoices_new.pkl", "expired_invoices.txt")
    extractor.load()
    df = extractor.extract()

    df.to_csv("results.csv", index=False)
    print(f"\nSaved {len(df)} rows to results.csv")



