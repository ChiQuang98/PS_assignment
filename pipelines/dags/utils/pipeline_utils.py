"""
Shared utility functions for data processing pipelines.
Contains database operations, validation functions, and configuration.
"""
import os
import logging
import pandas as pd
from datetime import datetime
from typing import Dict, Any, Optional
from sqlalchemy import create_engine
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.api.common.experimental.get_dag_runs import get_dag_runs
from utils.slack_alert import SlackAlert

# Set up logger
logger = logging.getLogger(__name__)

# Constants
INITIAL_INCREMENTAL_TIME = '1970-01-01 00:00:00'
AIRFLOW_HOME = os.getenv('AIRFLOW_HOME')
WAREHOUSE_HOST = os.getenv('WAREHOUSE_HOST')
WAREHOUSE_PASSWORD = os.getenv('WAREHOUSE_PASSWORD')
WAREHOUSE_USER = os.getenv('WAREHOUSE_USER')

# Initialize Slack client
SLACK_TOKEN = os.getenv("SLACK_ALERT_BOT_TOKEN")
CHANNEL_ID = os.getenv("CRITICAL_SLACK_CHANNEL")
AIRFLOW_IP_ADDRESS = os.getenv("AIRFLOW_DE_PROD_IP")
# Initialize Slack client
slack_alert = SlackAlert(slack_token=SLACK_TOKEN, channel_id=CHANNEL_ID)



# Database configuration constants
DB_CONFIG = {
    "raw": {
        "schema": "app_source",
        "tables": {
            "purchases": "purchases_data",
            "spins_hourly": "spins_data_hourly"
        }
    },
    "bronze": {
        "schema": "bronze_layer",
        "tables": {
            "purchases": "bronze_purchases",
            "spins_hourly": "bronze_spins_hourly"
        }
    },
    "silver": {
        "schema": "silver_layer",
        "tables": {
            "purchases": "silver_purchases",
            "spins_hourly": "silver_spins_hourly"
        }
    },
    "connections": {
        "source": "postgres_source_db",
        "warehouse": "postgres_warehouse_db"
    }
}

# Common DAG arguments
DEFAULT_ARGS = {
    'owner': 'Andrew Tran',
    'depends_on_past': False,
    'retries': 0,
    'email_on_failure': False,
    'email_on_retry': False,
}


def on_failure_callback(context):
    """Send Slack alert when task fails."""
    task_instance = context["task_instance"]
    execution_date = context["execution_date"].isoformat()
    
    log_url = context['task_instance'].log_url.replace("localhost:8080", AIRFLOW_IP_ADDRESS)

    ref_link_information = f"Log Dag Run URL: {log_url}"
    # Send to Slack using your existing SlackAlert class
    slack_alert.alert_failure(
        task_name=task_instance.task_id,
        dag_id=task_instance.dag_id,
        execution_date=str(execution_date),
        error_message=str(context.get("exception")),
        log_output="",  
        ref_link=ref_link_information,
    )

def validate_datetime(date_str: str) -> datetime:
    """
    Validates and converts a string date to datetime object.
    
    Args:
        date_str: Date string in format 'YYYY-MM-DD HH:MM:SS'
        
    Returns:
        Validated datetime object
        
    Raises:
        ValueError: If date format is invalid
    """
    try:
        return datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")
    except ValueError:
        error_msg = f"Invalid date format: {date_str}. Please use YYYY-MM-DD HH:MM:SS."
        logger.error(error_msg)
        raise ValueError(error_msg)


def extract_data_from_source(source_hook: PostgresHook, query: str) -> pd.DataFrame:
    """
    Extract data from a source database using a SQL query.
    
    Args:
        source_hook: PostgresHook or similar database hook
        query: SQL query to execute
        
    Returns:
        Query results as a DataFrame
        
    Raises:
        Exception: If query execution fails
    """
    logger.info(f"Executing query: {query}")
    conn = None
    try:
        conn = source_hook.get_conn()
        df = pd.read_sql(query, conn)
        logger.info(f"Query executed successfully. Retrieved {len(df)} rows.")
        return df
    except Exception as e:
        logger.error(f"Error executing query: {str(e)}")
        raise
    finally:
        if conn:
            conn.close()
            logger.debug("Database connection closed")


def get_latest_updated_at_from_table(
    source_hook: PostgresHook, 
    table_name: str, 
    schema_name: str,
    column_name: str = "updated_at"
) -> str:
    """
    Get the latest updated_at timestamp from a table.
    
    Args:
        source_hook: PostgresHook or similar database hook
        table_name: Table to query
        schema_name: Schema containing the table
        
    Returns:
        Latest timestamp in 'YYYY-MM-DD HH:MM:SS.fff' format
    """
    query = f"SELECT MAX({column_name}) FROM {schema_name}.{table_name}"
    logger.info(f"Getting latest {column_name} from {schema_name}.{table_name}")
    
    df = extract_data_from_source(source_hook, query)
    latest_updated_at = INITIAL_INCREMENTAL_TIME
    
    if not df.empty and df.iloc[0]['max'] is not None:
        latest_updated_at = df.iloc[0]['max'].strftime("%Y-%m-%d %H:%M:%S.%f")
        
    logger.info(f"Latest updated_at: {latest_updated_at}")
    return latest_updated_at


def load_data_to_warehouse(
    warehouse_hook: PostgresHook, 
    df: pd.DataFrame, 
    table_name: str, 
    schema_name: str, 
    if_exists: str = "append"
) -> int:
    """
    Load DataFrame into a data warehouse table.
    
    Args:
        warehouse_hook: PostgresHook or similar database hook
        df: Data to load
        table_name: Target table name
        schema_name: Target schema name
        if_exists: Strategy if table exists (append/replace/fail)
        
    Returns:
        Number of rows inserted
        
    Raises:
        Exception: If data loading fails
    """
    if df.empty:
        logger.info(f"No data to insert into {schema_name}.{table_name}")
        return 0
    
    logger.info(f"Loading {len(df)} rows into {schema_name}.{table_name}")
    try:
        engine = create_engine(warehouse_hook.get_uri().split("?")[0])
        with engine.connect() as conn:
            df.to_sql(
                table_name, 
                schema=schema_name, 
                con=conn, 
                if_exists=if_exists, 
                index=False
            )
        logger.info(f"Successfully loaded {len(df)} rows into {schema_name}.{table_name}")
        return len(df)
    except Exception as e:
        logger.error(f"Error loading data to {schema_name}.{table_name}: {str(e)}")
        raise


def is_dag_running(dag_id: str) -> bool:
    """
    Check if a DAG is currently running
    
    Args:
        dag_id: The DAG ID to check
        
    Returns:
        True if the DAG is running, False otherwise
    """
    try:
        dag_runs = get_dag_runs(dag_id=dag_id, state="running")
        logger.info(f"Found {len(dag_runs)} running instances of DAG {dag_id}")
        return len(dag_runs) > 0
    except Exception as e:
        logger.error(f"Error checking if DAG {dag_id} is running: {str(e)}")
        # Default to assuming it's not running if there's an error
        return False


def get_dbt_command(tag: str, full_refresh: bool = False) -> str:
    """
    Generate a dbt command string with proper environment variables.
    
    Args:
        tag: dbt model tag to run
        full_refresh: Whether to use the --full-refresh flag
        
    Returns:
        Formatted dbt command string
    """
    refresh_flag = "--full-refresh" if full_refresh else ""
    
    return (
        f"cd {AIRFLOW_HOME}/dbt/playstudio_project && "
        f"export WAREHOUSE_HOST={WAREHOUSE_HOST} && "
        f"export WAREHOUSE_PASSWORD={WAREHOUSE_PASSWORD} && "
        f"export WAREHOUSE_USER={WAREHOUSE_USER} && "
        f"dbt deps && "
        f"dbt run --select tag:{tag} --target prod {refresh_flag}"
    ).strip() 