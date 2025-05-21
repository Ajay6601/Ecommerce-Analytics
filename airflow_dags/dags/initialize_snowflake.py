from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
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
    'initialize_snowflake',
    default_args=default_args,
    description='Initialize Snowflake data warehouse',
    schedule_interval=None,  # Manual trigger
    start_date=datetime(2023, 1, 1),
    catchup=False
)

# SQL to create schema
create_schema_sql = """
CREATE DATABASE IF NOT EXISTS ECOMMERCE_ANALYTICS;
USE DATABASE ECOMMERCE_ANALYTICS;
CREATE SCHEMA IF NOT EXISTS PUBLIC;
"""

# SQL to create tables
create_tables_sql = """
-- Dimension Tables

-- Time Dimension
CREATE TABLE IF NOT EXISTS DIM_DATE (
    DATE_ID INTEGER AUTOINCREMENT PRIMARY KEY,
    DATE DATE UNIQUE NOT NULL,
    DAY INTEGER NOT NULL,
    MONTH INTEGER NOT NULL,
    YEAR INTEGER NOT NULL,
    DAY_NAME VARCHAR(10) NOT NULL,
    MONTH_NAME VARCHAR(10) NOT NULL,
    QUARTER INTEGER NOT NULL,
    IS_WEEKEND BOOLEAN NOT NULL,
    IS_HOLIDAY BOOLEAN DEFAULT FALSE
);

-- Customer Dimension
CREATE TABLE IF NOT EXISTS DIM_CUSTOMER (
    CUSTOMER_ID VARCHAR(50) PRIMARY KEY,
    FIRST_PURCHASE_DATE DATE NOT NULL,
    LAST_PURCHASE_DATE DATE NOT NULL,
    TOTAL_ORDERS INTEGER NOT NULL DEFAULT 0,
    TOTAL_SPEND NUMERIC(12, 2) NOT NULL DEFAULT 0,
    CUSTOMER_SEGMENT VARCHAR(20),
    IS_ACTIVE BOOLEAN DEFAULT TRUE,
    COUNTRIES ARRAY
);

-- Product Dimension
CREATE TABLE IF NOT EXISTS DIM_PRODUCT (
    PRODUCT_ID VARCHAR(50) PRIMARY KEY,
    PRODUCT_NAME VARCHAR(255) NOT NULL,
    CATEGORY VARCHAR(50),
    FIRST_SOLD_DATE DATE,
    LAST_SOLD_DATE DATE,
    IS_ACTIVE BOOLEAN DEFAULT TRUE,
    UNIT_PRICE NUMERIC(10, 2),
    TOTAL_UNITS_SOLD INTEGER DEFAULT 0,
    TOTAL_REVENUE NUMERIC(12, 2) DEFAULT 0
);

-- Fact Tables

-- Transactions Fact Table
CREATE TABLE IF NOT EXISTS FACT_TRANSACTIONS (
    TRANSACTION_ID INTEGER AUTOINCREMENT PRIMARY KEY,
    EVENT_ID VARCHAR(100) UNIQUE NOT NULL,
    CUSTOMER_ID VARCHAR(50) REFERENCES DIM_CUSTOMER(CUSTOMER_ID),
    PRODUCT_ID VARCHAR(50) REFERENCES DIM_PRODUCT(PRODUCT_ID),
    DATE_ID INTEGER REFERENCES DIM_DATE(DATE_ID),
    TIMESTAMP TIMESTAMP_NTZ NOT NULL,
    QUANTITY INTEGER NOT NULL,
    UNIT_PRICE NUMERIC(10, 2) NOT NULL,
    TOTAL_AMOUNT NUMERIC(12, 2) NOT NULL,
    IS_RETURN BOOLEAN DEFAULT FALSE,
    COUNTRY VARCHAR(50)
);

-- Aggregation Tables

-- Daily Sales
CREATE TABLE IF NOT EXISTS AGG_DAILY_SALES (
    DATE_ID INTEGER REFERENCES DIM_DATE(DATE_ID),
    TOTAL_REVENUE NUMERIC(12, 2) NOT NULL DEFAULT 0,
    TOTAL_ORDERS INTEGER NOT NULL DEFAULT 0,
    TOTAL_UNITS INTEGER NOT NULL DEFAULT 0,
    UNIQUE_CUSTOMERS INTEGER NOT NULL DEFAULT 0,
    AVG_ORDER_VALUE NUMERIC(10, 2),
    PRIMARY KEY (DATE_ID)
);

-- Customer Metrics
CREATE TABLE IF NOT EXISTS AGG_CUSTOMER_METRICS (
    CUSTOMER_ID VARCHAR(50) REFERENCES DIM_CUSTOMER(CUSTOMER_ID),
    YEAR INTEGER NOT NULL,
    MONTH INTEGER NOT NULL,
    REVENUE NUMERIC(12, 2) NOT NULL DEFAULT 0,
    ORDERS INTEGER NOT NULL DEFAULT 0,
    AVG_ORDER_VALUE NUMERIC(10, 2),
    PRIMARY KEY (CUSTOMER_ID, YEAR, MONTH)
);

-- Product Performance
CREATE TABLE IF NOT EXISTS AGG_PRODUCT_PERFORMANCE (
    PRODUCT_ID VARCHAR(50) REFERENCES DIM_PRODUCT(PRODUCT_ID),
    YEAR INTEGER NOT NULL,
    MONTH INTEGER NOT NULL,
    UNITS_SOLD INTEGER NOT NULL DEFAULT 0,
    REVENUE NUMERIC(12, 2) NOT NULL DEFAULT 0,
    RETURN_UNITS INTEGER NOT NULL DEFAULT 0,
    RETURN_RATE NUMERIC(5, 2),
    PRIMARY KEY (PRODUCT_ID, YEAR, MONTH)
);
"""

# Create schema task
create_schema = SnowflakeOperator(
    task_id='create_schema',
    sql=create_schema_sql,
    snowflake_conn_id='snowflake',
    dag=dag
)

# Create tables task
create_tables = SnowflakeOperator(
    task_id='create_tables',
    sql=create_tables_sql,
    snowflake_conn_id='snowflake',
    dag=dag
)

# Function to populate date dimension
def populate_date_dimension(**kwargs):
    """Populate the date dimension with dates for analysis"""
    # Get Snowflake connection details from Airflow
    conn = BaseHook.get_connection('snowflake')
    account = conn.extra_dejson.get('account')
    user = conn.login
    password = conn.password
    database = conn.extra_dejson.get('database', 'ECOMMERCE_ANALYTICS')
    warehouse = conn.extra_dejson.get('warehouse')
    schema = conn.extra_dejson.get('schema', 'PUBLIC')

    # Create dates for the past 2 years and next 1 year
    start_date = datetime.now() - timedelta(days=365*2)
    end_date = datetime.now() + timedelta(days=365)

    dates = []
    current_date = start_date

    while current_date <= end_date:
        date_entry = {
            'DATE': current_date.date(),
            'DAY': current_date.day,
            'MONTH': current_date.month,
            'YEAR': current_date.year,
            'DAY_NAME': current_date.strftime('%A'),
            'MONTH_NAME': current_date.strftime('%B'),
            'QUARTER': (current_date.month - 1) // 3 + 1,
            'IS_WEEKEND': current_date.weekday() >= 5,
            'IS_HOLIDAY': False
        }
        dates.append(date_entry)
        current_date += timedelta(days=1)

    # Convert to DataFrame
    df = pd.DataFrame(dates)

    # Connect to Snowflake
    ctx = snowflake.connector.connect(
        user=user,
        password=password,
        account=account,
        warehouse=warehouse,
        database=database,
        schema=schema
    )

    try:
        success, nchunks, nrows, _ = write_pandas(ctx, df, 'DIM_DATE', auto_create_table=False)
        logging.info(f"Loaded {nrows} dates to DIM_DATE in {nchunks} chunks")
    except Exception as e:
        logging.error(f"Error loading dates to Snowflake: {e}")
        for _, row in df.iterrows():
            try:
                cursor = ctx.cursor()
                cursor.execute(
                    f"""INSERT INTO DIM_DATE 
                        (DATE, DAY, MONTH, YEAR, DAY_NAME, MONTH_NAME, QUARTER, IS_WEEKEND, IS_HOLIDAY)
                        SELECT '{row['DATE']}', {row['DAY']}, {row['MONTH']}, {row['YEAR']},
                        '{row['DAY_NAME']}', '{row['MONTH_NAME']}', {row['QUARTER']},
                        {str(row['IS_WEEKEND']).upper()}, {str(row['IS_HOLIDAY']).upper()}
                        WHERE NOT EXISTS (SELECT 1 FROM DIM_DATE WHERE DATE = '{row['DATE']}')"""
                )
                cursor.close()
            except Exception:
                pass
    finally:
        ctx.close()

# Task to populate the date dimension
populate_dates = PythonOperator(
    task_id='populate_date_dimension',
    python_callable=populate_date_dimension,
    provide_context=True,
    dag=dag
)

# Task dependency chain
create_schema >> create_tables >> populate_dates
