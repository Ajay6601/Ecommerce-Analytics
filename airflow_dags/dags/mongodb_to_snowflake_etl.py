from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.hooks.base_hook import BaseHook
import pymongo
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import logging

# Default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# Define the DAG
dag = DAG(
    'mongodb_to_snowflake_etl',
    default_args=default_args,
    description='ETL from MongoDB to Snowflake',
    schedule_interval='0 1 * * *',  # Daily at 1 AM
    start_date=datetime(2023, 1, 1),
    catchup=False
)

# Helper function to get MongoDB connection
def get_mongodb():
    """Connect to MongoDB"""
    client = pymongo.MongoClient(
        "mongodb://admin:password@mongodb:27017/ecommerce_analytics?authSource=admin"
    )
    return client.ecommerce_analytics

# Helper function to get Snowflake connection
def get_snowflake():
    """Connect to Snowflake"""
    conn = BaseHook.get_connection('snowflake')
    return snowflake.connector.connect(
        user=conn.login,
        password=conn.password,
        account=conn.extra_dejson.get('account'),
        warehouse=conn.extra_dejson.get('warehouse'),
        database='ECOMMERCE_ANALYTICS',
        schema='PUBLIC'
    )

# Task function to load customer dimension
def load_customer_dimension(**kwargs):
    """ETL for customer dimension"""
    mongo_db = get_mongodb()

    # Extract customer data
    pipeline = [
        {"$match": {"event_type": {"$regex": "purchase"}}},
        {"$group": {
            "_id": "$user_id",
            "first_purchase": {"$min": "$timestamp"},
            "last_purchase": {"$max": "$timestamp"},
            "total_orders": {"$sum": 1},
            "total_spend": {"$sum": "$metadata.total_amount"},
            "countries": {"$addToSet": "$metadata.country"}
        }}
    ]

    customers = list(mongo_db.processed_events.aggregate(pipeline, allowDiskUse=True))

    if customers:
        # Transform data
        df_customers = []

        for customer in customers:
            # Skip if no customer ID
            if not customer['_id']:
                continue

            # Parse dates
            first_purchase_date = customer['first_purchase'][:10]
            last_purchase_date = customer['last_purchase'][:10]

            # Determine segment - simple logic
            if customer['total_spend'] > 1000:
                segment = 'High Value'
            elif customer['total_orders'] > 10:
                segment = 'Loyal'
            elif customer['total_orders'] == 1:
                segment = 'New'
            else:
                segment = 'Regular'

            # Determine active status
            is_active = (datetime.now() - datetime.strptime(last_purchase_date, '%Y-%m-%d')).days <= 90

            df_customers.append({
                'CUSTOMER_ID': customer['_id'],
                'FIRST_PURCHASE_DATE': first_purchase_date,
                'LAST_PURCHASE_DATE': last_purchase_date,
                'TOTAL_ORDERS': customer['total_orders'],
                'TOTAL_SPEND': customer['total_spend'],
                'CUSTOMER_SEGMENT': segment,
                'IS_ACTIVE': is_active,
                'COUNTRIES': customer.get('countries', [])
            })

        # Convert to DataFrame
        df = pd.DataFrame(df_customers)

        # Load to Snowflake
        conn = get_snowflake()
        try:
            # Clear existing data (for simplicity in demo)
            conn.cursor().execute("DELETE FROM DIM_CUSTOMER")

            # Load new data
            success, nchunks, nrows, _ = write_pandas(conn, df, 'DIM_CUSTOMER')
            logging.info(f"Loaded {nrows} customers to Snowflake")
        except Exception as e:
            logging.error(f"Error loading customer dimension: {e}")
        finally:
            conn.close()
    else:
        logging.warning("No customer data found in MongoDB")

# Task function to load product dimension
def load_product_dimension(**kwargs):
    """ETL for product dimension"""
    mongo_db = get_mongodb()

    # Extract product data
    pipeline = [
        {"$match": {"event_type": {"$regex": "purchase|return"}}},
        {"$group": {
            "_id": "$metadata.stock_code",
            "product_name": {"$first": "$metadata.description"},
            "first_sold_date": {"$min": "$timestamp"},
            "last_sold_date": {"$max": "$timestamp"},
            "units_sold": {
                "$sum": {
                    "$cond": [
                        {"$eq": ["$event_type", "return"]},
                        {"$multiply": ["$metadata.quantity", -1]},
                        "$metadata.quantity"
                    ]
                }
            },
            "total_revenue": {
                "$sum": {
                    "$cond": [
                        {"$eq": ["$event_type", "return"]},
                        {"$multiply": ["$metadata.total_amount", -1]},
                        "$metadata.total_amount"
                    ]
                }
            },
            "avg_unit_price": {"$avg": "$metadata.unit_price"}
        }}
    ]

    products = list(mongo_db.processed_events.aggregate(pipeline, allowDiskUse=True))

    if products:
        # Transform data
        df_products = []

        for product in products:
            if not product['_id'] or not product['product_name']:
                continue

            # Get dates
            first_sold_date = product['first_sold_date'][:10]
            last_sold_date = product['last_sold_date'][:10]

            # Determine category
            product_name = product['product_name'].lower() if isinstance(product['product_name'], str) else ""
            if any(keyword in product_name for keyword in ['heart', 'candle', 'frame', 'sign']):
                category = 'Home Decor'
            elif any(keyword in product_name for keyword in ['cup', 'mug', 'plate', 'kitchen']):
                category = 'Kitchen'
            elif any(keyword in product_name for keyword in ['garden', 'outdoor']):
                category = 'Garden'
            elif any(keyword in product_name for keyword in ['gift', 'christmas', 'party']):
                category = 'Gift & Party'
            elif any(keyword in product_name for keyword in ['box', 'storage', 'tin']):
                category = 'Storage'
            else:
                category = 'Other'

            # Active status
            is_active = (datetime.now() - datetime.strptime(last_sold_date, '%Y-%m-%d')).days <= 180

            df_products.append({
                'PRODUCT_ID': product['_id'],
                'PRODUCT_NAME': product['product_name'],
                'CATEGORY': category,
                'FIRST_SOLD_DATE': first_sold_date,
                'LAST_SOLD_DATE': last_sold_date,
                'IS_ACTIVE': is_active,
                'UNIT_PRICE': product['avg_unit_price'],
                'TOTAL_UNITS_SOLD': product['units_sold'],
                'TOTAL_REVENUE': product['total_revenue']
            })

        # Convert to DataFrame
        df = pd.DataFrame(df_products)

        # Load to Snowflake
        conn = get_snowflake()
        try:
            # Clear existing data
            conn.cursor().execute("DELETE FROM DIM_PRODUCT")

            # Load new data
            success, nchunks, nrows, _ = write_pandas(conn, df, 'DIM_PRODUCT')
            logging.info(f"Loaded {nrows} products to Snowflake")
        except Exception as e:
            logging.error(f"Error loading product dimension: {e}")
        finally:
            conn.close()
    else:
        logging.warning("No product data found in MongoDB")

# Task function to load fact transactions
def load_fact_transactions(**kwargs):
    """ETL for fact transactions"""
    mongo_db = get_mongodb()
    conn = get_snowflake()
    cursor = conn.cursor()

    try:
        # Get date mappings from Snowflake
        cursor.execute("SELECT DATE_ID, DATE FROM DIM_DATE")
        date_mapping = {row[1].strftime('%Y-%m-%d'): row[0] for row in cursor.fetchall()}

        # Get latest transaction timestamp
        cursor.execute("SELECT MAX(TIMESTAMP) FROM FACT_TRANSACTIONS")
        result = cursor.fetchone()
        last_timestamp = result[0] if result and result[0] else '2000-01-01'

        # Extract new transactions from MongoDB
        transactions = mongo_db.processed_events.find(
            {
                "event_type": {"$regex": "purchase|return"},
                "timestamp": {"$gt": last_timestamp}
            },
            {
                "event_id": 1,
                "user_id": 1,
                "timestamp": 1,
                "event_type": 1,
                "metadata.stock_code": 1,
                "metadata.quantity": 1,
                "metadata.unit_price": 1,
                "metadata.total_amount": 1,
                "metadata.country": 1
            }
        ).limit(10000)  # Process in batches

        # Transform transactions
        records = []
        for tx in transactions:
            if 'metadata' not in tx:
                continue

            # Get date components
            date_str = tx['timestamp'][:10]
            if date_str not in date_mapping:
                continue

            # Skip if missing fields
            if not all(k in tx['metadata'] for k in ['stock_code', 'quantity', 'unit_price', 'total_amount']):
                continue

            records.append({
                'EVENT_ID': tx['event_id'],
                'CUSTOMER_ID': tx['user_id'],
                'PRODUCT_ID': tx['metadata']['stock_code'],
                'DATE_ID': date_mapping[date_str],
                'TIMESTAMP': tx['timestamp'],
                'QUANTITY': tx['metadata']['quantity'],
                'UNIT_PRICE': tx['metadata']['unit_price'],
                'TOTAL_AMOUNT': tx['metadata']['total_amount'],
                'IS_RETURN': 'return' in tx['event_type'].lower(),
                'COUNTRY': tx['metadata'].get('country', 'Unknown')
            })

        if records:
            df = pd.DataFrame(records)

            # Load to Snowflake
            success, nchunks, nrows, _ = write_pandas(conn, df, 'FACT_TRANSACTIONS')
            logging.info(f"Loaded {nrows} transactions to Snowflake")
        else:
            logging.info("No new transactions to load")

    except Exception as e:
        logging.error(f"Error loading transactions: {e}")
    finally:
        conn.close()

# Function to update aggregation tables
def update_aggregations(**kwargs):
    """Update aggregation tables in Snowflake"""
    conn = get_snowflake()
    cursor = conn.cursor()

    try:
        # Update AGG_DAILY_SALES
        cursor.execute("""
        MERGE INTO AGG_DAILY_SALES target
        USING (
            SELECT
                t.DATE_ID,
                SUM(CASE WHEN NOT t.IS_RETURN THEN t.TOTAL_AMOUNT ELSE 0 END) AS TOTAL_REVENUE,
                COUNT(DISTINCT CASE WHEN NOT t.IS_RETURN THEN t.EVENT_ID END) AS TOTAL_ORDERS,
                SUM(CASE WHEN NOT t.IS_RETURN THEN t.QUANTITY ELSE 0 END) AS TOTAL_UNITS,
                COUNT(DISTINCT t.CUSTOMER_ID) AS UNIQUE_CUSTOMERS,
                CASE WHEN COUNT(DISTINCT CASE WHEN NOT t.IS_RETURN THEN t.EVENT_ID END) > 0 
                     THEN SUM(CASE WHEN NOT t.IS_RETURN THEN t.TOTAL_AMOUNT ELSE 0 END) / 
                          COUNT(DISTINCT CASE WHEN NOT t.IS_RETURN THEN t.EVENT_ID END)
                     ELSE 0 
                END AS AVG_ORDER_VALUE
            FROM FACT_TRANSACTIONS t
            GROUP BY t.DATE_ID
        ) source
        ON target.DATE_ID = source.DATE_ID
        WHEN MATCHED THEN
            UPDATE SET
                target.TOTAL_REVENUE = source.TOTAL_REVENUE,
                target.TOTAL_ORDERS = source.TOTAL_ORDERS,
                target.TOTAL_UNITS = source.TOTAL_UNITS,
                target.UNIQUE_CUSTOMERS = source.UNIQUE_CUSTOMERS,
                target.AVG_ORDER_VALUE = source.AVG_ORDER_VALUE
        WHEN NOT MATCHED THEN
            INSERT (DATE_ID, TOTAL_REVENUE, TOTAL_ORDERS, TOTAL_UNITS, UNIQUE_CUSTOMERS, AVG_ORDER_VALUE)
            VALUES (source.DATE_ID, source.TOTAL_REVENUE, source.TOTAL_ORDERS, source.TOTAL_UNITS, 
                    source.UNIQUE_CUSTOMERS, source.AVG_ORDER_VALUE)
        """)
        logging.info("Updated AGG_DAILY_SALES")

        # Update AGG_CUSTOMER_METRICS
        cursor.execute("""
        MERGE INTO AGG_CUSTOMER_METRICS target
        USING (
            SELECT
                t.CUSTOMER_ID,
                YEAR(t.TIMESTAMP) AS YEAR,
                MONTH(t.TIMESTAMP) AS MONTH,
                SUM(CASE WHEN NOT t.IS_RETURN THEN t.TOTAL_AMOUNT ELSE 0 END) AS REVENUE,
                COUNT(DISTINCT CASE WHEN NOT t.IS_RETURN THEN t.EVENT_ID END) AS ORDERS,
                CASE WHEN COUNT(DISTINCT CASE WHEN NOT t.IS_RETURN THEN t.EVENT_ID END) > 0 
                     THEN SUM(CASE WHEN NOT t.IS_RETURN THEN t.TOTAL_AMOUNT ELSE 0 END) / 
                          COUNT(DISTINCT CASE WHEN NOT t.IS_RETURN THEN t.EVENT_ID END)
                     ELSE 0 
                END AS AVG_ORDER_VALUE
            FROM FACT_TRANSACTIONS t
            GROUP BY t.CUSTOMER_ID, YEAR(t.TIMESTAMP), MONTH(t.TIMESTAMP)
        ) source
        ON target.CUSTOMER_ID = source.CUSTOMER_ID AND target.YEAR = source.YEAR AND target.MONTH = source.MONTH
        WHEN MATCHED THEN
            UPDATE SET
                target.REVENUE = source.REVENUE,
                target.ORDERS = source.ORDERS,
                target.AVG_ORDER_VALUE = source.AVG_ORDER_VALUE
        WHEN NOT MATCHED THEN
            INSERT (CUSTOMER_ID, YEAR, MONTH, REVENUE, ORDERS, AVG_ORDER_VALUE)
            VALUES (source.CUSTOMER_ID, source.YEAR, source.MONTH, source.REVENUE, source.ORDERS, source.AVG_ORDER_VALUE)
        """)
        logging.info("Updated AGG_CUSTOMER_METRICS")

        # Update AGG_PRODUCT_PERFORMANCE
        cursor.execute("""
        MERGE INTO AGG_PRODUCT_PERFORMANCE target
        USING (
            SELECT
                t.PRODUCT_ID,
                YEAR(t.TIMESTAMP) AS YEAR,
                MONTH(t.TIMESTAMP) AS MONTH,
                SUM(CASE WHEN NOT t.IS_RETURN THEN t.QUANTITY ELSE 0 END) AS UNITS_SOLD,
                SUM(CASE WHEN NOT t.IS_RETURN THEN t.TOTAL_AMOUNT ELSE 0 END) AS REVENUE,
                SUM(CASE WHEN t.IS_RETURN THEN ABS(t.QUANTITY) ELSE 0 END) AS RETURN_UNITS,
                CASE 
                    WHEN SUM(CASE WHEN NOT t.IS_RETURN THEN t.QUANTITY ELSE 0 END) > 0 
                    THEN (SUM(CASE WHEN t.IS_RETURN THEN ABS(t.QUANTITY) ELSE 0 END) / 
                          SUM(CASE WHEN NOT t.IS_RETURN THEN t.QUANTITY ELSE 0 END)) * 100
                    ELSE 0 
                END AS RETURN_RATE
            FROM FACT_TRANSACTIONS t
            GROUP BY t.PRODUCT_ID, YEAR(t.TIMESTAMP), MONTH(t.TIMESTAMP)
        ) source
        ON target.PRODUCT_ID = source.PRODUCT_ID AND target.YEAR = source.YEAR AND target.MONTH = source.MONTH
        WHEN MATCHED THEN
            UPDATE SET
                target.UNITS_SOLD = source.UNITS_SOLD,
                target.REVENUE = source.REVENUE,
                target.RETURN_UNITS = source.RETURN_UNITS,
                target.RETURN_RATE = source.RETURN_RATE
        WHEN NOT MATCHED THEN
            INSERT (PRODUCT_ID, YEAR, MONTH, UNITS_SOLD, REVENUE, RETURN_UNITS, RETURN_RATE)
            VALUES (source.PRODUCT_ID, source.YEAR, source.MONTH, source.UNITS_SOLD, 
                    source.REVENUE, source.RETURN_UNITS, source.RETURN_RATE)
        """)
        logging.info("Updated AGG_PRODUCT_PERFORMANCE")

    except Exception as e:
        logging.error(f"Error updating aggregations: {e}")
    finally:
        conn.close()

# Task to update MongoDB collections
def update_mongodb_collections(**kwargs):
    """Update MongoDB collections for pre-aggregated metrics"""
    mongo_db = get_mongodb()

    # 1. Update hourly_revenue collection
    pipeline = [
        {"$match": {"event_type": {"$regex": "purchase"}}},
        {"$project": {
            "hour": {"$substr": ["$timestamp", 0, 13]},
            "amount": "$metadata.total_amount",
            "user_id": 1
        }},
        {"$group": {
            "_id": "$hour",
            "total_revenue": {"$sum": "$amount"},
            "order_count": {"$sum": 1},
            "unique_customers": {"$addToSet": "$user_id"}
        }},
        {"$project": {
            "hour_timestamp": "$_id",
            "total_revenue": 1,
            "order_count": 1,
            "unique_customers": {"$size": "$unique_customers"}
        }}
    ]

    try:
        hourly_results = list(mongo_db.processed_events.aggregate(pipeline, allowDiskUse=True))

        if hourly_results:
            # Clear existing data
            mongo_db.hourly_revenue.delete_many({})

            # Insert new data
            mongo_db.hourly_revenue.insert_many(hourly_results)
            logging.info(f"Updated hourly_revenue with {len(hourly_results)} records")
    except Exception as e:
        logging.error(f"Error updating hourly_revenue: {e}")

    # 2. Update popular_products collection
    pipeline = [
        {"$match": {"event_type": {"$regex": "purchase"}}},
        {"$group": {
            "_id": {"id": "$metadata.stock_code", "name": "$metadata.description"},
            "total_quantity": {"$sum": "$metadata.quantity"},
            "total_revenue": {"$sum": "$metadata.total_amount"},
            "order_count": {"$sum": 1}
        }},
        {"$sort": {"total_revenue": -1}},
        {"$limit": 100}
    ]

    try:
        product_results = list(mongo_db.processed_events.aggregate(pipeline, allowDiskUse=True))

        if product_results:
            # Transform to desired format
            products_to_insert = []

            for p in product_results:
                if not p["_id"]["id"] or not p["_id"]["name"]:
                    continue

                products_to_insert.append({
                    "product_id": p["_id"]["id"],
                    "product_name": p["_id"]["name"],
                    "total_quantity": p["total_quantity"],
                    "total_revenue": p["total_revenue"],
                    "order_count": p["order_count"],
                    "updated_at": datetime.now()
                })

            if products_to_insert:
                # Clear existing data
                mongo_db.popular_products.delete_many({})

                # Insert new data
                mongo_db.popular_products.insert_many(products_to_insert)
                logging.info(f"Updated popular_products with {len(products_to_insert)} products")
    except Exception as e:
        logging.error(f"Error updating popular_products: {e}")

    # 3. Update product_recommendations collection
    pipeline = [
        {"$match": {"event_type": {"$regex": "purchase"}}},
        {"$group": {
            "_id": "$metadata.invoice_no",
            "products": {"$push": {
                "id": "$metadata.stock_code",
                "name": "$metadata.description"
            }}
        }},
        {"$match": {"products.1": {"$exists": True}}}
    ]

    try:
        orders = list(mongo_db.processed_events.aggregate(pipeline, allowDiskUse=True))

        if orders:
            # Calculate co-occurrences
            product_pairs = {}
            product_counts = {}

            for order in orders:
                products = order["products"]

                # Count individual products
                for product in products:
                    if not product["id"]:
                        continue

                    if product["id"] not in product_counts:
                        product_counts[product["id"]] = 0
                    product_counts[product["id"]] += 1

                # Count pairs
                for i in range(len(products)):
                    for j in range(i+1, len(products)):
                        p1 = products[i]
                        p2 = products[j]

                        if not p1["id"] or not p2["id"]:
                            continue

                        # Create pair key (smaller ID first)
                        pair = (p1["id"], p2["id"]) if p1["id"] < p2["id"] else (p2["id"], p1["id"])

                        if pair not in product_pairs:
                            product_pairs[pair] = {
                                "count": 0,
                                "products": [p1["name"], p2["name"]]
                            }
                        product_pairs[pair]["count"] += 1

            # Generate recommendations
            total_orders = len(orders)
            recommendations = {}

            for pair, data in product_pairs.items():
                p1, p2 = pair
                count = data["count"]

                # Skip rare combinations
                if count < 3:
                    continue

                p1_count = product_counts.get(p1, 0)
                p2_count = product_counts.get(p2, 0)

                # Calculate lift
                expected = (p1_count / total_orders) * (p2_count / total_orders) * total_orders
                lift = count / expected if expected > 0 else 0

                # Add recommendation for p1
                if p1 not in recommendations:
                    recommendations[p1] = {
                        "product_id": p1,
                        "product_name": data["products"][0],
                        "recommendations": []
                    }

                recommendations[p1]["recommendations"].append({
                    "product_id": p2,
                    "product_name": data["products"][1],
                    "co_purchases": count,
                    "lift": lift
                })

                # Add recommendation for p2
                if p2 not in recommendations:
                    recommendations[p2] = {
                        "product_id": p2,
                        "product_name": data["products"][1],
                        "recommendations": []
                    }

                recommendations[p2]["recommendations"].append({
                    "product_id": p1,
                    "product_name": data["products"][0],
                    "co_purchases": count,
                    "lift": lift
                })

            # Sort and limit recommendations
            recommendation_docs = []
            for product_id, rec_data in recommendations.items():
                # Sort by lift (highest first)
                rec_data["recommendations"].sort(key=lambda x: x["lift"], reverse=True)

                # Keep top 5
                rec_data["recommendations"] = rec_data["recommendations"][:5]

                # Add timestamp
                rec_data["updated_at"] = datetime.now()

                recommendation_docs.append(rec_data)

            if recommendation_docs:
                # Clear existing data
                mongo_db.product_recommendations.delete_many({})

                # Insert new data
                mongo_db.product_recommendations.insert_many(recommendation_docs)
                logging.info(f"Updated product_recommendations with {len(recommendation_docs)} products")
    except Exception as e:
        logging.error(f"Error updating product_recommendations: {e}")

# Define tasks for each ETL step
task_load_customers = PythonOperator(
    task_id='load_customers',
    python_callable=load_customer_dimension,
    provide_context=True,
    dag=dag
)

task_load_products = PythonOperator(
    task_id='load_products',
    python_callable=load_product_dimension,
    provide_context=True,
    dag=dag
)

task_load_transactions = PythonOperator(
    task_id='load_transactions',
    python_callable=load_fact_transactions,
    provide_context=True,
    dag=dag
)

task_update_aggregations = PythonOperator(
    task_id='update_aggregations',
    python_callable=update_aggregations,
    provide_context=True,
    dag=dag
)

task_update_mongodb = PythonOperator(
    task_id='update_mongodb_collections',
    python_callable=update_mongodb_collections,
    provide_context=True,
    dag=dag
)

# DBT task to run transformations
dbt_run_task = BashOperator(
    task_id='run_dbt',
    bash_command='cd /opt/airflow/dags/dbt && dbt run --profiles-dir .',
    dag=dag
)

# Define task dependencies - dimensional model approach
[task_load_customers, task_load_products] >> task_load_transactions >> task_update_aggregations >> task_update_mongodb >> dbt_run_task