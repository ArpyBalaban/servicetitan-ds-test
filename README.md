# Customer Order Data Extraction and Flattening

## Overview

This project implements a `CustomerDataExtractor` class in Python to load, clean, and transform nested customer order data into a flat tabular format suitable for analysis.

The input data consists of:

- A pickled file (`customer_orders.pkl`) containing customer records. Each record includes:
  - Customer ID, name, and registration date
  - A list of orders, each with order ID, order date, shipping address, and items
  - Each item has item ID, product name, category, price, and quantity

- A text file (`vip_customers.txt`) listing customer IDs marked as VIPs.

The goal is to extract and flatten this nested data into a single DataFrame with these columns:

| Column                        | Type             | Description                                  |
|-------------------------------|------------------|----------------------------------------------|
| `customer_id`                 | int              | Unique ID of the customer                     |
| `customer_name`               | string           | Customer's full name                          |
| `registration_date`           | datetime64[ns]   | Customer registration date                    |
| `is_vip`                     | bool             | Whether the customer is a VIP                 |
| `order_id`                    | int              | Unique order identifier                        |
| `order_date`                  | datetime64[ns]   | Date the order was placed                      |
| `product_id`                  | int              | Unique product/item identifier                 |
| `product_name`                | string           | Product/item name                              |
| `category`                    | string           | Product category (mapped from integers with fallback to 'Misc') |
| `unit_price`                  | float            | Price per unit of the item                     |
| `item_quantity`               | int              | Quantity of this item in the order             |
| `total_item_price`            | float            | Total price for this item (unit_price × item_quantity), rounded to 2 decimals |
| `total_order_value_percentage` | float          | Percentage contribution of this item’s total price to the overall order value, rounded to 2 decimals |

---

## Key Features and Handling

- **Robust Data Parsing:** Handles various data inconsistencies including:
  - Extracting numeric IDs from mixed-format strings (e.g., `"ORD84"` → `84`), ensuring all IDs are integers.
  - Sanitizing price fields that may contain currency symbols (e.g., `"$377.96"`) or invalid strings (`"FREE"`, `"INVALID"`), converting to numeric values or zero.
  - Parsing quantity fields with possible non-numeric values similarly.
  - Including zero-item orders as rows with null values (`NaN` or `<NA>`) in item-related columns, preserving order-level information.
  - Validating dates to detect future or unrealistic dates, setting invalid dates as missing (`NaT`).
  - Logging missing or malformed data, and skipping records only when critical fields are absent.
  
- **Detailed Error Reporting:** 
  - Skipped customers, orders, and items are tracked with reasons and exported as CSV files in a `logs/` directory (`skipped_customers.csv`, `skipped_orders.csv`, `skipped_items.csv`).

- **Data Type Enforcement:** Final DataFrame strictly matches the specification with nullable types where appropriate.

- **Sorting:** Final output is sorted ascending by `customer_id`, then `order_id`, then `product_id`.

- **Numeric Rounding:** Monetary columns `total_item_price` and `total_order_value_percentage` are rounded to 2 decimal places for consistency.

- **Data Quality Summary Report:**
  - A summary text file `data_quality_report.txt` is generated after processing.
  - It includes counts of processed and skipped customers, orders, and items.
  - Displays number of VIP customers detected.
  - Reports zero-item orders count.
  - Shows product category distribution with counts and percentages.
  - The report is printed to console and saved to file for auditing.

- **Export Function:** Exports the final flattened DataFrame as a CSV file (`customer_orders_flattened.csv`) matching the exact column order and types required.

---

## Usage Instructions

### Requirements

- Python 3.8+
- Libraries:
  - pandas
  - numpy

### Running the Extraction

1. Ensure the following files are in your working directory:

   - `customer_orders.pkl`
   - `vip_customers.txt`

2. Run the extraction script (`data_loader.py` or your `.py` file):

```bash
python data_loader.py

Acknowledgements
This solution was developed with assistance from OpenAI's ChatGPT language model.
