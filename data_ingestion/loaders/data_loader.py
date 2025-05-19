import pandas as pd
import numpy as np
from datetime import datetime
import json
import os
import argparse
import sys

# Add project root to Python path to enable imports
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '../..'))
sys.path.insert(0, project_root)

# Import logging configuration
from config.logging_config import setup_logging, get_logger

# Set up logging
setup_logging()
logger = get_logger('data_ingestion.loaders.uk_retail_loader')

class UKRetailLoader:
    def __init__(self, data_path, output_path=None, include_anonymous=True):
        self.data_path = data_path
        self.output_path = output_path
        self.include_anonymous = include_anonymous

        # Validate input path
        if not os.path.exists(self.data_path):
            raise FileNotFoundError(f"Input file not found: {self.data_path}")

        # Create output directory if it doesn't exist and output path is specified
        if self.output_path:
            output_dir = os.path.dirname(self.output_path)
            if output_dir:
                os.makedirs(output_dir, exist_ok=True)

    def load_data(self):
        logger.info(f"Loading data from {self.data_path}")

        try:
            # Load the CSV file
            data = pd.read_csv(self.data_path, encoding='unicode_escape')
            initial_row_count = len(data)
            logger.info(f"Initial dataset contains {initial_row_count} records")

            # Convert data types
            data['InvoiceDate'] = pd.to_datetime(data['InvoiceDate'])
            data['CustomerID'] = pd.to_numeric(data['CustomerID'], errors='coerce')

            # Handle missing CustomerIDs
            missing_mask = data['CustomerID'].isna()
            missing_count = missing_mask.sum()

            if missing_count > 0:
                if self.include_anonymous:
                    # Generate anonymous IDs starting from -1, -2, etc.
                    anon_ids = range(-1, -missing_count-1, -1)
                    data.loc[missing_mask, 'CustomerID'] = list(anon_ids)
                    logger.info(f"Assigned anonymous IDs to {missing_count} records with missing CustomerID")
                else:
                    # Drop records with missing CustomerID
                    data = data.dropna(subset=['CustomerID'])
                    logger.info(f"Removed {missing_count} records with missing CustomerID")

            # Apply data quality filters with detailed logging
            initial_count = len(data)

            # Remove extreme quantity values
            data_filtered = data[(data['Quantity'] > -100000) & (data['Quantity'] < 100000)]
            quantity_filtered = initial_count - len(data_filtered)
            if quantity_filtered > 0:
                logger.info(f"Removed {quantity_filtered} records with extreme Quantity values")

            # Remove invalid unit prices
            price_filtered = len(data_filtered) - len(data_filtered[(data_filtered['UnitPrice'] > 0.0) & (data_filtered['UnitPrice'] < 100000)])
            if price_filtered > 0:
                logger.info(f"Removed {price_filtered} records with invalid UnitPrice values")

            data = data_filtered[(data_filtered['UnitPrice'] > 0.0) & (data_filtered['UnitPrice'] < 100000)]

            # Add derived columns
            data['TotalAmount'] = data['Quantity'] * data['UnitPrice']

            # Identify returns (negative quantities or invoice starting with 'C')
            data['IsReturn'] = (data['Quantity'] < 0) | (data['InvoiceNo'].astype(str).str.startswith('C'))

            # Log return statistics
            return_count = data[data['IsReturn']].shape[0]
            return_percentage = (return_count / len(data)) * 100
            logger.info(f"Dataset contains {return_count} returns ({return_percentage:.1f}% of total)")

            logger.info(f"Final dataset: {len(data)} records after all preprocessing")
            return data

        except Exception as e:
            logger.error(f"Error loading data: {e}", exc_info=True)
            raise

    def transform_to_events(self, df):
        logger.info("Transforming transactions to events with detailed categorization")
        events = []

        try:
            # Create a lookup dictionary for previous purchases by customer
            customer_purchases = {}
            df = df.sort_values('InvoiceDate')
            event_type_counts = {}

            for idx, row in df.iterrows():
                # Handle both actual and anonymous customer IDs
                customer_id = f"customer_{int(row['CustomerID'])}"
                is_return = row['IsReturn']
                quantity = row['Quantity']
                total_amount = row['TotalAmount']

                # Determine base event type
                if is_return:
                    # Subcategorize returns
                    if str(row['InvoiceNo']).startswith('C'):
                        event_type = 'credit_note_return'
                    elif abs(quantity) == customer_purchases.get((customer_id, row['StockCode']), {}).get('quantity', 0):
                        event_type = 'full_return'
                    else:
                        event_type = 'partial_return'
                else:
                    # Subcategorize purchases
                    if customer_id not in customer_purchases:
                        event_type = 'first_purchase'
                    elif quantity > 10:
                        event_type = 'bulk_purchase'
                    elif total_amount > 100:
                        event_type = 'high_value_purchase'
                    else:
                        event_type = 'repeat_purchase'

                    # Track this purchase for return analysis
                    if customer_id not in customer_purchases:
                        customer_purchases[customer_id] = {}
                    customer_purchases[customer_id][row['StockCode']] = {
                        'quantity': quantity,
                        'invoice_no': row['InvoiceNo']
                    }

                event_type_counts[event_type] = event_type_counts.get(event_type, 0) + 1

                # Create the event
                event = {
                    'event_id': f"{row['InvoiceNo']}_{row['StockCode']}",
                    'event_type': event_type,
                    'user_id': customer_id,
                    'timestamp': row['InvoiceDate'].isoformat(),
                    'metadata': {
                        'invoice_no': str(row['InvoiceNo']),
                        'stock_code': str(row['StockCode']),
                        'description': str(row['Description']),
                        'quantity': int(row['Quantity']),
                        'unit_price': float(row['UnitPrice']),
                        'total_amount': float(row['TotalAmount']),
                        'country': str(row['Country']),
                        'is_return': bool(is_return)
                    }
                }

                # Add customer type (identified vs anonymous)
                if int(row['CustomerID']) < 0:
                    event['metadata']['customer_type'] = 'anonymous'
                else:
                    event['metadata']['customer_type'] = 'identified'

                # Add seasonal tagging
                month = row['InvoiceDate'].month
                if month in [11, 12]:
                    event['metadata']['season'] = 'holiday'
                elif month in [3, 4, 5]:
                    event['metadata']['season'] = 'spring'
                elif month in [6, 7, 8]:
                    event['metadata']['season'] = 'summer'
                else:
                    event['metadata']['season'] = 'fall'

                events.append(event)

                # Log progress for large datasets
                if idx > 0 and idx % 10000 == 0:
                    progress_percentage = (idx / len(df)) * 100
                    logger.info(f"Processed {idx:,}/{len(df):,} records ({progress_percentage:.1f}%)")

            # Log event type distribution
            logger.info("Event type distribution:")
            for event_type, count in event_type_counts.items():
                percentage = (count / len(events)) * 100
                logger.info(f"  - {event_type}: {count:,} events ({percentage:.1f}%)")

            # Log anonymous vs. identified statistics
            anonymous_events = [e for e in events if e['metadata']['customer_type'] == 'anonymous']
            anonymous_percentage = (len(anonymous_events) / len(events)) * 100
            logger.info(f"Anonymous customers: {len(anonymous_events):,} events ({anonymous_percentage:.1f}%)")

            logger.info(f"Transformed {len(events):,} events successfully")
            return events

        except Exception as e:
            logger.error(f"Error transforming events: {e}", exc_info=True)
            raise

    def save_events(self, events):
        if not self.output_path:
            logger.warning("No output path specified, skipping save")
            return

        try:
            os.makedirs(os.path.dirname(self.output_path), exist_ok=True)

            logger.info(f"Saving {len(events):,} events to {self.output_path}")
            with open(self.output_path, 'w') as f:
                json.dump(events, f)

            file_size_mb = os.path.getsize(self.output_path) / (1024 * 1024)
            logger.info(f"Events saved successfully. File size: {file_size_mb:.2f} MB")

        except Exception as e:
            logger.error(f"Error saving events to {self.output_path}: {e}", exc_info=True)
            raise

    def process(self):
        logger.info("Starting UK Retail Dataset processing")
        start_time = datetime.now()

        try:
            df = self.load_data()
            events = self.transform_to_events(df)
            if self.output_path:
                self.save_events(events)
            processing_time = (datetime.now() - start_time).total_seconds()
            events_per_second = len(events) / processing_time if processing_time > 0 else 0
            logger.info(f"Processing completed in {processing_time:.2f} seconds ({events_per_second:.1f} events/second)")

            return events

        except Exception as e:
            logger.error(f"Error processing UK Retail Dataset: {e}", exc_info=True)
            raise


def parse_args():
    parser = argparse.ArgumentParser(description='UK Retail Dataset Loader')
    parser.add_argument('--input', type=str, required=True, help='Path to UK Retail CSV file')
    parser.add_argument('--output', type=str, default=None, help='Path to save processed events (JSON)')
    parser.add_argument('--include-anonymous', action='store_true', help='Include transactions with anonymous customers')
    return parser.parse_args()


if __name__ == "__main__":
    try:
        input_path = "C:/Users/ajayr/PycharmProjects/Ecommerce-Analytics/data/raw/data.csv"
        output_path = "C:/Users/ajayr/PycharmProjects/Ecommerce-Analytics/data/processed/events.json"
        include_anonymous = True
        loader = UKRetailLoader(input_path, output_path, include_anonymous)
        events = loader.process()
        print(f"Processed {len(events):,} events successfully")
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        print(f"Error: {e}")
        sys.exit(1)