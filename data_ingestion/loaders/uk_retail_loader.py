import pandas as pd
import numpy as np
from datetime import datetime
import json
import os
import argparse
import logging

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("uk_retail_loader")


class UKRetailLoader:
    def __init__(self, data_path, output_path=None):
        """
        Initialize the UK Retail Dataset loader

        Args:
            data_path (str): Path to the UK Retail CSV file
            output_path (str, optional): Path to save processed data
        """
        self.data_path = data_path
        self.output_path = output_path

    def load_data(self):
        """Load the UK Retail Dataset from CSV"""
        logger.info(f"Loading data from {self.data_path}")

        # Load the CSV file
        df = pd.read_csv(self.data_path, encoding='unicode_escape')

        # Convert data types
        df['InvoiceDate'] = pd.to_datetime(df['InvoiceDate'])
        df['CustomerID'] = pd.to_numeric(df['CustomerID'], errors='coerce')

        # Handle missing values
        df = df.dropna(subset=['CustomerID'])

        # Add derived columns
        df['TotalAmount'] = df['Quantity'] * df['UnitPrice']

        # Identify returns (negative quantities or invoice starting with 'C')
        df['IsReturn'] = (df['Quantity'] < 0) | (df['InvoiceNo'].astype(str).str.startswith('C'))

        logger.info(f"Loaded {len(df)} records")
        return df

    def transform_to_events(self, df):
        """Transform transactions into event format"""
        logger.info("Transforming transactions to events")
        events = []

        for idx, row in df.iterrows():
            event = {
                'event_id': f"{row['InvoiceNo']}_{row['StockCode']}",
                'event_type': 'return' if row['IsReturn'] else 'purchase',
                'user_id': f"customer_{int(row['CustomerID'])}",
                'timestamp': row['InvoiceDate'].isoformat(),
                'metadata': {
                    'invoice_no': str(row['InvoiceNo']),
                    'stock_code': str(row['StockCode']),
                    'description': str(row['Description']),
                    'quantity': int(row['Quantity']),
                    'unit_price': float(row['UnitPrice']),
                    'total_amount': float(row['TotalAmount']),
                    'country': str(row['Country'])
                }
            }
            events.append(event)

            # Log progress for large datasets
            if idx > 0 and idx % 10000 == 0:
                logger.info(f"Processed {idx} records")

        logger.info(f"Transformed {len(events)} events")
        return events

    def save_events(self, events):
        """Save events to JSON file"""
        if not self.output_path:
            logger.warning("No output path specified, skipping save")
            return

        # Create output directory if it doesn't exist
        os.makedirs(os.path.dirname(self.output_path), exist_ok=True)

        logger.info(f"Saving {len(events)} events to {self.output_path}")
        with open(self.output_path, 'w') as f:
            json.dump(events, f)

        logger.info("Events saved successfully")

    def process(self):
        """Process the UK Retail Dataset end-to-end"""
        # Load the data
        df = self.load_data()

        # Transform to events
        events = self.transform_to_events(df)

        # Save events if output path is specified
        if self.output_path:
            self.save_events(events)

        return events


def parse_args():
    parser = argparse.ArgumentParser(description='UK Retail Dataset Loader')
    parser.add_argument('--input', type=str, required=True, help='Path to UK Retail CSV file')
    parser.add_argument('--output', type=str, default=None, help='Path to save processed events (JSON)')
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()

    loader = UKRetailLoader(args.input, args.output)
    events = loader.process()

    print(f"Processed {len(events)} events")