Certainly! Here's the README in proper markdown format for GitHub, including a credit to ChatGPT at the end.

markdown
Copy
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
| `total_item_price`            | float            | Total price for this item (unit_price × item_quantity) |
| `total_order_value_percentage` | float          | Percentage contribution of this item’s total price to the overall order value |

---

## Key Features and Handling

- **Robust Data Parsing:** Handles various data inconsistencies including:
  - Mixed-type IDs (e.g., `"ORD84"`), extracting numeric parts to keep IDs as integers.
  - Price fields with currency symbols (e.g., `"$377.96"`) or invalid strings (`"FREE"`, `"INVALID"`) by sanitizing and converting to numeric or zero.
  - Quantity fields with non-numeric values handled similarly.
  - Orders with zero items are included as rows with item-related columns set to null (NaN or `<NA>`), preserving order-level information.
  - Missing or malformed records are logged with warnings and skipped where critical.
  
- **Data Type Enforcement:** Ensures final DataFrame strictly matches the specification with appropriate nullable types for columns that may contain missing values.

- **Sorting:** The final DataFrame is sorted ascending by `customer_id`, then `order_id`, then `product_id`.

- **Export to CSV:** Includes a method to export the flattened DataFrame as a CSV file exactly matching the required format.

---

## Usage Instructions

### Requirements

- Python 3.8+
- Libraries:
  - pandas
  - numpy

### Running the Extraction

1. Place the files `customer_orders.pkl` and `vip_customers.txt` in the working directory.

2. Run the extraction script (`data_loader.py` or your `.py` file):

```bash
python data_loader.py
This will load the data, parse and flatten it, then save the resulting DataFrame as:

Copy
customer_orders_flattened.csv
Sample output and logs will be printed to the console for inspection.

File Structure
graphql
Copy
├── customer_orders.pkl           # Pickled nested customer order data
├── vip_customers.txt             # Text file listing VIP customer IDs
├── data_loader.py                # Python script with CustomerDataExtractor class and execution code
├── customer_orders_flattened.csv # Output flattened CSV file
└── README.md                    # This documentation file
Notes
The CustomerDataExtractor class includes detailed logging of data issues for traceability.

The code gracefully handles missing or malformed data by skipping or sanitizing values without interrupting the whole process.

Numeric IDs are extracted from mixed-format strings to meet strict integer ID requirements.

The export function ensures the CSV columns and types strictly follow the specification.

Acknowledgements
This solution was developed with the assistance of OpenAI's ChatGPT language model.
