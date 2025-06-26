import pickle
import logging
import pandas as pd
import numpy as np
import re

class CustomerDataExtractor:
    CATEGORY_MAP = {1: 'Electronics', 2: 'Apparel', 3: 'Books', 4: 'Home Goods'}

    def __init__(self, vip_file: str, data_file: str, log_level=logging.WARNING):
        self.vip_file = vip_file
        self.data_file = data_file
        self.vip_customers = set()
        self.customer_orders = []
        logging.basicConfig(level=log_level, format='%(levelname)s: %(message)s')
        self.logger = logging.getLogger(__name__)

    @staticmethod
    def extract_int_from_str(value):
        """
        Extract the first integer found in a string or return None if no integer found.
        If input is already int, return as is.
        """
        if isinstance(value, int):
            return value
        if isinstance(value, float):
            return int(value)
        if isinstance(value, str):
            match = re.search(r'\d+', value)
            if match:
                return int(match.group())
        return None

    @staticmethod
    def parse_price(price):
        if price is None:
            return np.nan
        if isinstance(price, (int, float)):
            return float(price)
        if isinstance(price, str):
            price_clean = price.replace('$', '').replace(',', '').strip()
            if price_clean.upper() in ('FREE', '', 'INVALID', 'NONE'):
                return 0.0
            try:
                return float(price_clean)
            except ValueError:
                return np.nan
        return np.nan

    @staticmethod
    def parse_quantity(quantity):
        if quantity is None:
            return np.nan
        if isinstance(quantity, int):
            return quantity
        if isinstance(quantity, float):
            return int(quantity)
        if isinstance(quantity, str):
            quantity_clean = quantity.strip().upper()
            if quantity_clean in ('FREE', '', 'INVALID', 'NONE'):
                return 0
            try:
                return int(quantity_clean)
            except ValueError:
                return np.nan
        return np.nan

    def load_vip_customers(self):
        try:
            with open(self.vip_file, 'r') as f:
                for line in f:
                    line = line.strip()
                    if line.isdigit():
                        self.vip_customers.add(int(line))
                    else:
                        self.logger.warning(f"Skipping invalid VIP ID line: {line}")
            self.logger.info(f"Loaded {len(self.vip_customers)} VIP customer IDs.")
        except Exception as e:
            self.logger.error(f"Failed to load VIP customers from {self.vip_file}: {e}")
            raise

    def load_customer_orders(self):
        try:
            with open(self.data_file, 'rb') as f:
                self.customer_orders = pickle.load(f)
            self.logger.info(f"Loaded {len(self.customer_orders)} customer records.")
        except Exception as e:
            self.logger.error(f"Failed to load customer orders from {self.data_file}: {e}")
            raise

    def flatten_data(self) -> pd.DataFrame:
        rows = []

        for cust_idx, cust in enumerate(self.customer_orders):
            try:
                cust_id = cust.get('id')
                cust_name = cust.get('name')
                reg_date_raw = cust.get('registration_date')

                if cust_id is None or cust_name is None or reg_date_raw is None:
                    self.logger.warning(f"Missing customer info in record index {cust_idx}, skipping customer.")
                    continue

                try:
                    reg_date = pd.to_datetime(reg_date_raw)
                except Exception:
                    self.logger.warning(f"Invalid registration_date for customer {cust_id}, setting as NaT.")
                    reg_date = pd.NaT

                is_vip = cust_id in self.vip_customers

                orders = cust.get('orders', [])
                if not isinstance(orders, list):
                    self.logger.warning(f"Orders field malformed for customer {cust_id}, skipping.")
                    continue

                for order_idx, order in enumerate(orders):
                    raw_order_id = order.get('order_id')
                    order_id = self.extract_int_from_str(raw_order_id)
                    order_date_raw = order.get('order_date')

                    if order_id is None or order_date_raw is None:
                        self.logger.warning(f"Missing or invalid order_id/date for customer {cust_id}, order index {order_idx}, skipping order.")
                        continue

                    try:
                        order_date = pd.to_datetime(order_date_raw)
                    except Exception:
                        self.logger.warning(f"Invalid order_date for customer {cust_id} order {order_id}, setting as NaT.")
                        order_date = pd.NaT

                    items = order.get('items', [])
                    if not isinstance(items, list):
                        self.logger.warning(f"Items field malformed for customer {cust_id} order {order_id}, treating as empty list.")
                        items = []

                    # Calculate total order value
                    item_total_prices = []
                    for item in items:
                        try:
                            price = self.parse_price(item.get('price', 0.0))
                            quantity = self.parse_quantity(item.get('quantity', 0))
                            total_item_price = price * quantity
                            item_total_prices.append(total_item_price)
                        except Exception:
                            self.logger.warning(f"Invalid price or quantity in item for customer {cust_id} order {order_id}, counting item total price as 0.")
                            item_total_prices.append(0.0)
                    total_order_value = sum(item_total_prices)

                    if len(items) == 0:
                        # Zero-item order: one row with NaNs in item columns
                        rows.append({
                            'customer_id': int(cust_id),
                            'customer_name': str(cust_name),
                            'registration_date': reg_date,
                            'is_vip': is_vip,
                            'order_id': int(order_id),
                            'order_date': order_date,
                            'product_id': pd.NA,
                            'product_name': pd.NA,
                            'category': pd.NA,
                            'unit_price': np.nan,
                            'item_quantity': pd.NA,
                            'total_item_price': np.nan,
                            'total_order_value_percentage': np.nan,
                        })
                    else:
                        for idx, item in enumerate(items):
                            try:
                                raw_product_id = item.get('item_id')
                                product_id = self.extract_int_from_str(raw_product_id)
                                product_name = item.get('product_name')
                                raw_category = item.get('category')
                                unit_price = self.parse_price(item.get('price', np.nan))
                                item_quantity = self.parse_quantity(item.get('quantity', np.nan))

                                category = self.CATEGORY_MAP.get(raw_category, 'Misc')

                                if None in (product_id, product_name) or pd.isna(unit_price) or pd.isna(item_quantity):
                                    self.logger.warning(f"Missing item info for customer {cust_id} order {order_id}, item index {idx}. Skipping item.")
                                    continue

                                total_item_price = unit_price * item_quantity
                                if total_order_value > 0:
                                    total_order_value_percentage = (total_item_price / total_order_value) * 100
                                else:
                                    total_order_value_percentage = np.nan

                                rows.append({
                                    'customer_id': int(cust_id),
                                    'customer_name': str(cust_name),
                                    'registration_date': reg_date,
                                    'is_vip': is_vip,
                                    'order_id': int(order_id),
                                    'order_date': order_date,
                                    'product_id': int(product_id),
                                    'product_name': str(product_name),
                                    'category': category,
                                    'unit_price': float(unit_price),
                                    'item_quantity': int(item_quantity),
                                    'total_item_price': float(total_item_price),
                                    'total_order_value_percentage': float(total_order_value_percentage),
                                })

                            except Exception as e:
                                self.logger.warning(f"Error processing item for customer {cust_id} order {order_id} item index {idx}: {e}")
                                continue

            except Exception as e:
                self.logger.error(f"Error processing customer index {cust_idx}: {e}")
                continue

        if not rows:
            self.logger.warning("No valid data rows extracted.")
            return pd.DataFrame()

        df = pd.DataFrame(rows)

        # Enforce data types strictly
        df = df.astype({
            'customer_id': 'int64',
            'customer_name': 'string',
            'registration_date': 'datetime64[ns]',
            'is_vip': 'bool',
            'order_id': 'int64',
            'order_date': 'datetime64[ns]',
            'product_id': 'Int64',
            'product_name': 'string',
            'category': 'string',
            'unit_price': 'float64',
            'item_quantity': 'Int64',
            'total_item_price': 'float64',
            'total_order_value_percentage': 'float64'
        })

        df = df.sort_values(by=['customer_id', 'order_id', 'product_id'], ascending=True).reset_index(drop=True)
        return df
    def save_to_csv(self, df: pd.DataFrame, filename: str):
        """
        Save the given DataFrame to CSV with exact column order and no index.
        """
        # Define exact output columns in order
        output_columns = [
            'customer_id',
            'customer_name',
            'registration_date',
            'is_vip',
            'order_id',
            'order_date',
            'product_id',
            'product_name',
            'category',
            'unit_price',
            'item_quantity',
            'total_item_price',
            'total_order_value_percentage'
        ]

        # Ensure DataFrame has the columns in the right order
        df_to_save = df[output_columns]

        # Save CSV without index
        df_to_save.to_csv(filename, index=False)
        self.logger.info(f"Saved flattened data to CSV file: {filename}")

if __name__ == "__main__":
    extractor = CustomerDataExtractor(vip_file='vip_customers.txt', data_file='customer_orders.pkl', log_level=logging.INFO)
    extractor.load_vip_customers()
    extractor.load_customer_orders()
    df = extractor.flatten_data()

    print("\nSample of extracted flattened data:")
    print(df.head(10))

    # Save to CSV file
    extractor.save_to_csv(df, 'customer_orders_flattened.csv')
