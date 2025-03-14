import pandas as pd
import psycopg2
from psycopg2 import sql
from dotenv import load_dotenv
from utils.excel_utils import read_excel_file
import os

# Load environment variables
load_dotenv()

# Database Configuration
DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": os.getenv("DB_PORT", "5432"),
    "user": os.getenv("DB_USER", "airflow"),
    "password": os.getenv("DB_PASSWORD", "airflow"),
}

# Connect to PostgreSQL and execute a query
def execute_query(conn, query):
    with conn.cursor() as cursor:
        cursor.execute(query)
        conn.commit()

# Function to create databases and schemas
def setup_databases():
    conn = psycopg2.connect(**DB_CONFIG, dbname="postgres")
    conn.autocommit = True
    with conn.cursor() as cursor:
        cursor.execute("SELECT 1 FROM pg_database WHERE datname = 'source_db'")
        if not cursor.fetchone():
            cursor.execute("CREATE DATABASE source_db")

        cursor.execute("SELECT 1 FROM pg_database WHERE datname = 'warehouse_db'")
        if not cursor.fetchone():
            cursor.execute("CREATE DATABASE warehouse_db")
    conn.close()

    # Connect to source_db and create schemas/tables
    conn = psycopg2.connect(**DB_CONFIG, dbname="source_db")
    execute_query(conn, "CREATE SCHEMA IF NOT EXISTS app_source;")
    
    execute_query(conn, """
        CREATE TABLE IF NOT EXISTS app_source.spins_data_hourly (
            timestamp TIMESTAMP NULL,
            user_id TEXT NULL,
            country CHAR(4) NULL,
            total_spins FLOAT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP 
        );
    """)
    
    execute_query(conn, """
        CREATE TABLE IF NOT EXISTS app_source.purchases_data (
            timestamp TIMESTAMP NULL,
            user_id TEXT NULL,
            revenue TEXT NULL,
            transaction_id TEXT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP 
        );
    """)
    conn.close()

    # Connect to warehouse_db and create schemas/tables
    conn = psycopg2.connect(**DB_CONFIG, dbname="warehouse_db")
    execute_query(conn, "CREATE SCHEMA IF NOT EXISTS bronze_layer;")
    execute_query(conn, "CREATE SCHEMA IF NOT EXISTS silver_layer;")
    execute_query(conn, "CREATE SCHEMA IF NOT EXISTS gold_layer;")
    
    # Create bronze tables
    execute_query(conn, """
        CREATE TABLE IF NOT EXISTS bronze_layer.bronze_spins_hourly (
            timestamp TIMESTAMP NULL,
            user_id TEXT NULL,
            country CHAR(4) NULL,
            total_spins FLOAT NULL,
            extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP 
        );
    """)
    
    execute_query(conn, """
        CREATE TABLE IF NOT EXISTS bronze_layer.bronze_purchases (
            timestamp TIMESTAMP NULL,
            user_id TEXT NULL,
            revenue TEXT NULL,
            transaction_id TEXT NULL,
            extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP 
        );
    """)
    
    conn.close()

# Function to insert data into PostgreSQL tables
def insert_data(df, table_name, schema_name, conn):
    execute_query(conn, f"TRUNCATE TABLE {schema_name}.{table_name} RESTART IDENTITY CASCADE;")
    with conn.cursor() as cursor:
        for _, row in df.iterrows():
            columns = ", ".join(row.index)
            values = ", ".join(["%s"] * len(row))
            insert_query = sql.SQL(
                f"INSERT INTO {schema_name}.{table_name} ({columns}) VALUES ({values})"
            )
            cursor.execute(insert_query, tuple(row))
        conn.commit()
        print(f"Inserted {len(df)} records into {schema_name}.{table_name}")

# Main script
def main():
    setup_databases()

    # Read Excel file
    data_source_path = os.path.abspath(os.getenv("DATA_SOURCE_PATH", "MOC.xlsx"))
    spins_df = read_excel_file(
        file_path=data_source_path,
        sheet_name="Spins Hourly",
        usecols=["date", "userId", "country", "total_spins"]
    )
    purchases_df = read_excel_file(
        file_path=data_source_path,
        sheet_name="Purchases",
        usecols=["date", "userId", "revenue", "transaction_id"]
    )

    # Rename columns to match DB schema
    spins_df.rename(columns={"DATE": "timestamp", "USERID": "user_id"}, inplace=True)
    purchases_df.rename(columns={"DATE": "timestamp", "USERID": "user_id"}, inplace=True)

    # Insert data into PostgreSQL
    conn = psycopg2.connect(**DB_CONFIG, dbname="source_db")
    insert_data(spins_df, "spins_data_hourly", "app_source", conn)
    insert_data(purchases_df, "purchases_data", "app_source", conn)
    conn.close()

    print("Data successfully inserted into PostgreSQL.")

if __name__ == "__main__":
    main()