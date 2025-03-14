"""
DAG for processing data into bronze and silver layers with incremental and full refresh capabilities.
Runs every 5 minutes and can trigger the hourly processing pipeline when needed.
"""
# Standard library imports
import logging
from datetime import datetime, timedelta
from typing import Dict, Any

# Airflow imports
from airflow.decorators import task, dag
from airflow.operators.python import BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule

# Import shared utilities
from utils.pipeline_utils import (
    DB_CONFIG, DEFAULT_ARGS, INITIAL_INCREMENTAL_TIME,
    validate_datetime, extract_data_from_source, get_latest_updated_at_from_table,
    load_data_to_warehouse, is_dag_running, get_dbt_command, on_failure_callback
)

# Set up logger
logger = logging.getLogger(__name__)

HOURLY_DAG_ID = "hourly_processing_pipeline"
warehouse_connection_id = DB_CONFIG["connections"]["warehouse"]
source_connection_id = DB_CONFIG["connections"]["source"]
DEFAULT_ARGS['retries'] = 1
DEFAULT_ARGS['retry_delay'] = timedelta(minutes=1)

@dag(
    dag_id="five_mins_processing_pipeline",
    schedule_interval="*/5 * * * *",  # Runs every 5 minutes
    start_date=days_ago(1),
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    catchup=False,
    params={
        "is_full_refresh": False,
        "backfill_start_date": None,
        "backfill_end_date": None,
        "template_date": "2025-03-11 00:00:00"
    },
    tags=["incremental_ingestion"],
    dagrun_timeout=timedelta(minutes=8),
    is_paused_upon_creation=False,
    on_failure_callback=on_failure_callback
)
def five_mins_processing_pipeline():
    """
    DAG for ingesting data into the bronze layer with configurable incremental or full refresh modes.
    
    This DAG supports two modes:
    1. Incremental loading: Processes only new data since the last run
    2. Full refresh: Processes all data within a specified date range
    
    The DAG also checks if hourly data is up-to-date and triggers the hourly DAG if needed
    """
    
    @task(task_id="check_refresh_mode_and_validate")
    def check_refresh_mode_and_validate(**context) -> Dict[str, Any]:
        """
        Combined task: Check refresh mode and validate dates if needed
        
        Returns:
            Dictionary with refresh mode information and validation results
        """
        params = context.get('params', {})
        is_full_refresh = params.get('is_full_refresh', False)
        logger.info(f"Refresh mode: {'Full Refresh' if is_full_refresh else 'Incremental'}")
        
        # Check for first run condition
        warehouse_hook = PostgresHook(postgres_conn_id=warehouse_connection_id)
        latest_updated_at = get_latest_updated_at_from_table(
            warehouse_hook, 
            table_name=DB_CONFIG["bronze"]["tables"]["purchases"], 
            schema_name=DB_CONFIG["bronze"]["schema"],
            column_name="timestamp"
        )
        
        # If this is the first run with no data, force full refresh
        is_first_run = latest_updated_at == INITIAL_INCREMENTAL_TIME
        if is_first_run:
            logger.info("First run detected - using full refresh mode")
            is_full_refresh = True
            return {
                "is_full_refresh": is_full_refresh,
                "latest_updated_at": latest_updated_at,
                "is_first_run": is_first_run,
                "is_valid": True
            }
        
        # If full refresh but not first run, validate date parameters
        if is_full_refresh and not is_first_run:
            backfill_start_date = params.get('backfill_start_date')
            backfill_end_date = params.get('backfill_end_date')
            logger.info(f"Full refresh mode - date range: {backfill_start_date} to {backfill_end_date}")
            
            # Check if dates are provided
            if backfill_start_date is None or backfill_end_date is None:
                logger.error("Error: backfill_start_date and backfill_end_date must be provided for full refresh")
                return {
                    "is_full_refresh": is_full_refresh,
                    "latest_updated_at": latest_updated_at,
                    "is_first_run": is_first_run,
                    "is_valid": False
                }
            
            # Validate date format and range
            try:
                start_date = validate_datetime(backfill_start_date)
                end_date = validate_datetime(backfill_end_date)
                
                # Check if start date is before end date
                if start_date > end_date:
                    logger.error(f"Invalid date range: start date {start_date} is after end date {end_date}")
                    return {
                        "is_full_refresh": is_full_refresh,
                        "latest_updated_at": latest_updated_at,
                        "is_first_run": is_first_run,
                        "is_valid": False
                    }
                else:
                    logger.info(f"Validated date range: {start_date} to {end_date}")
                    return {
                        "is_full_refresh": is_full_refresh,
                        "latest_updated_at": latest_updated_at,
                        "is_first_run": is_first_run,
                        "is_valid": True,
                        "start_date": backfill_start_date,
                        "end_date": backfill_end_date
                    }
            except ValueError as e:
                logger.error(f"Date validation error: {e}")
                return {
                    "is_full_refresh": is_full_refresh,
                    "latest_updated_at": latest_updated_at,
                    "is_first_run": is_first_run,
                    "is_valid": False
                }
        
        # If incremental mode, no validation needed
        return {
            "is_full_refresh": is_full_refresh,
            "latest_updated_at": latest_updated_at,
            "is_first_run": is_first_run,
            "is_valid": True
        }
    
    def decide_extract_task(**context) -> str:
        """
        Decide whether to proceed with extraction or skip
        
        Returns:
            Task ID to execute next
        """
        ti = context['ti']
        
        # Get refresh mode info
        refresh_info = ti.xcom_pull(task_ids="check_refresh_mode_and_validate")
        
        # Check if validation passed
        if not refresh_info["is_valid"]:
            logger.info("Skipping extraction due to invalid parameters")
            return "end_dag"
        
        logger.info("Proceeding with extraction")
        return "extract_and_load_purchases"
    
    def decide_transform_task(**context) -> str:
        """
        Decide which transform task to run based on refresh mode
        
        Returns:
            Task ID of the transform task to execute
        """
        ti = context['ti']
        refresh_info = ti.xcom_pull(task_ids="check_refresh_mode_and_validate")
        
        if refresh_info["is_full_refresh"]:
            logger.info("Selected transform task: transform_silver_full_refresh")
            return "transform_silver_full_refresh"
        else:
            logger.info("Selected transform task: transform_silver_5mins_incremental")
            return "transform_silver_5mins_incremental"
    
    @task(task_id="check_hourly_data_freshness", trigger_rule=TriggerRule.ONE_SUCCESS)
    def check_hourly_data_freshness(**context) -> Dict[str, Any]:
        """
        Check if hourly data is up-to-date and determine if it needs a full refresh
        
        Returns:
            Dictionary with hourly data refresh requirements
        """
        ti = context['ti']
        # Get the refresh mode from the 5-minute pipeline
        five_min_refresh_info = ti.xcom_pull(task_ids="check_refresh_mode_and_validate")
        five_min_is_full_refresh = five_min_refresh_info["is_full_refresh"]
        is_valid = five_min_refresh_info["is_valid"]
        logger.info(f"5 mins pipeline is in: {'Full Refresh' if five_min_is_full_refresh else 'Incremental'} mode")
        
        # Hourly pipeline needs to be in full refresh mode if 5 mins pipeline is in full refresh mode
        if five_min_is_full_refresh and is_valid:
            # Get start_date and end_date if they exist in the refresh_info
            start_date = five_min_refresh_info.get("start_date", None)
            end_date = five_min_refresh_info.get("end_date", None)
            
            logger.info("Hourly pipeline needs to be in full refresh mode with time range same as 5 mins pipeline")
            
            return {
                "needs_hourly_refresh": True,
                "is_full_refresh": True,
                "5mins_is_full_refresh": five_min_is_full_refresh,
                "5mins_start_date": start_date,
                "5mins_end_date": end_date
            }
            
        # Connect to the data warehouse
        warehouse_hook = PostgresHook(postgres_conn_id=warehouse_connection_id)
        
        # Query the latest updated_at timestamp from hourly metrics table
        hourly_schema = DB_CONFIG["bronze"]["schema"]
        hourly_table = DB_CONFIG["bronze"]["tables"]["spins_hourly"]
        
        # Get the latest timestamp from the hourly metrics table
        latest_hourly_update = get_latest_updated_at_from_table(
            warehouse_hook,
            table_name=hourly_table,
            schema_name=hourly_schema,
            column_name="timestamp"
        )
        
        if latest_hourly_update == INITIAL_INCREMENTAL_TIME:
            logger.info("No hourly data found - hourly pipeline should be triggered with full refresh mode")
            return {
                "needs_hourly_refresh": True,
                "is_full_refresh": True,
                "5mins_is_full_refresh": five_min_is_full_refresh,
                "5mins_start_date": five_min_refresh_info.get("start_date", None),
                "5mins_end_date": five_min_refresh_info.get("end_date", None),
                "latest_update": latest_hourly_update
            }
            
        # Check if the latest update is within the expected freshness window
        # For hourly data, we expect data to be at most 1 hour old
        current_time = datetime.now()
        freshness_threshold = current_time - timedelta(hours=1)
        latest_hourly_update_dt = datetime.strptime(latest_hourly_update, "%Y-%m-%d %H:%M:%S.%f")
        needs_refresh = latest_hourly_update_dt < freshness_threshold
        
        # If 5-minute pipeline is in full refresh mode, hourly pipeline should also use full refresh
        hourly_full_refresh = five_min_is_full_refresh
        
        if needs_refresh:
            msg = f"Hourly data is stale. Latest update: {latest_hourly_update_dt}, Threshold: {freshness_threshold}"
            if hourly_full_refresh:
                msg += " - Will trigger with FULL REFRESH due to 5-min pipeline being in full refresh mode"
            logger.info(msg)
        else:
            logger.info(f"Hourly data is up-to-date. Latest update: {latest_hourly_update_dt}")
            
        return {
            "needs_hourly_refresh": needs_refresh,
            "is_full_refresh": hourly_full_refresh,
            "5mins_is_full_refresh": five_min_is_full_refresh,
            "5mins_start_date": five_min_refresh_info.get("start_date", None),
            "5mins_end_date": five_min_refresh_info.get("end_date", None),
            "latest_update": latest_hourly_update
        }
    
    def decide_hourly_pipeline(**context) -> str:
        """
        Decide whether to trigger the hourly pipeline
        
        Returns:
            Task ID to execute next
        """
        ti = context['ti']
        hourly_check_result = ti.xcom_pull(task_ids="check_hourly_data_freshness")
        
        # First check if data needs refresh
        if not hourly_check_result["needs_hourly_refresh"]:
            logger.info("Hourly data is up-to-date - skipping hourly pipeline")
            return "end_dag"
            
        # Then check if the hourly DAG is already running
        if is_dag_running(HOURLY_DAG_ID):
            logger.info(f"Hourly DAG {HOURLY_DAG_ID} is already running - skipping trigger")
            return "end_dag"
            
        # If data needs refresh and DAG is not running, trigger it
        logger.info("Hourly data needs refresh and DAG is not running - triggering hourly pipeline")
        return "trigger_hourly_pipeline"
    
    
    @task(task_id="extract_and_load_purchases")
    def extract_and_load_purchases(**context) -> str:
        """
        Extract purchases data from PostgreSQL and load it into the Bronze Layer.
        
        Returns:
            Result message with number of rows inserted
        """
        # Initialize task instance and database connections
        ti = context['ti']
        pg_source_hook = PostgresHook(postgres_conn_id=source_connection_id)
        warehouse_hook = PostgresHook(postgres_conn_id=warehouse_connection_id)
        
        # Get refresh mode information
        refresh_info = ti.xcom_pull(task_ids="check_refresh_mode_and_validate")
        is_full_refresh = refresh_info["is_full_refresh"]
        logger.info(f"Running extract_and_load_purchases in {'full refresh' if is_full_refresh else 'incremental'} mode")
        
        # Build the WHERE condition based on refresh type
        if is_full_refresh:
            if refresh_info["is_first_run"]:
                # First run with no data - get all data
                condition = "1=1"
                logger.info("First run - fetching all data")
            else:
                # Backfill mode: Use provided date range
                backfill_start_date = refresh_info.get("start_date")
                backfill_end_date = refresh_info.get("end_date")
                if not backfill_start_date or not backfill_end_date:
                    error_msg = "Missing date range for full refresh"
                    logger.error(error_msg)
                    return f"Error: {error_msg}"
                condition = f"timestamp >= '{backfill_start_date}' AND timestamp <= '{backfill_end_date}'"
                logger.info(f"Full refresh with date range: {backfill_start_date} to {backfill_end_date}")
        else:
            # Incremental mode: Use last processed timestamp
            incremental_time = refresh_info["latest_updated_at"]
            condition = f"timestamp > '{incremental_time}'"
            logger.info(f"Incremental mode - fetching data updated after: {incremental_time}")

        # Build and execute the query
        raw_schema = DB_CONFIG["raw"]["schema"]
        raw_table = DB_CONFIG["raw"]["tables"]["purchases"]
        query = f"""
            SELECT *, NOW() as extracted_at
            FROM {raw_schema}.{raw_table}
            WHERE {condition}
        """
        
        # Extract data
        try:
            df = extract_data_from_source(pg_source_hook, query)
            logger.info(f"Retrieved {len(df)} rows of data")
            
            # Additional validation logic
            if len(df) > 0:
                logger.info(f"Date range in result: {df['updated_at'].min()} to {df['updated_at'].max()}")
            
            # Load data into data warehouse
            bronze_schema = DB_CONFIG["bronze"]["schema"]
            bronze_table = DB_CONFIG["bronze"]["tables"]["purchases"]
            
            rows_inserted = load_data_to_warehouse(
                warehouse_hook, 
                df, 
                bronze_table, 
                bronze_schema
            )
            
            result_msg = f"Inserted {rows_inserted} rows into {bronze_schema}.{bronze_table}"
            logger.info(result_msg)
            return result_msg
            
        except Exception as e:
            logger.error(f"Error in extract_and_load_purchases: {str(e)}")
            raise
    
    # Define task instances
    
    # Start and end markers
    start_task = DummyOperator(task_id='start_dag')  
    end_task = DummyOperator(
        task_id='end_dag',
        trigger_rule=TriggerRule.ALL_DONE  # Ensure this task runs regardless of upstream success/failure
    )
    
    # Task flow steps - combined check_refresh_mode and validate_full_refresh_dates
    check_mode = check_refresh_mode_and_validate()
    
    # Decision tasks
    branch_to_extract_or_skip = BranchPythonOperator(
        task_id="branch_to_extract_or_skip",
        python_callable=decide_extract_task
    )
    
    branch_to_transform_task = BranchPythonOperator(
        task_id="branch_to_transform_task",
        python_callable=decide_transform_task,
        trigger_rule=TriggerRule.ONE_SUCCESS
    )
    
    # dbt tasks for transforming data to silver layer
    transform_silver_5mins_incremental = BashOperator(
        task_id="transform_silver_5mins_incremental",
        bash_command=get_dbt_command(tag="silver_5mins_incremental", full_refresh=False)
    )
    
    transform_silver_full_refresh = BashOperator(
        task_id="transform_silver_full_refresh",
        bash_command=get_dbt_command(tag="silver_5mins_incremental", full_refresh=True)
    )
    
    # Define main data fetching task
    extract_and_load_task = extract_and_load_purchases()
    
    # New hourly pipeline tasks
    check_hourly_freshness = check_hourly_data_freshness()
    
    branch_hourly_pipeline = BranchPythonOperator(
        task_id="branch_hourly_pipeline",
        python_callable=decide_hourly_pipeline
    )
    
    # Trigger hourly pipeline
    trigger_hourly_pipeline = TriggerDagRunOperator(
        task_id="trigger_hourly_pipeline",
        trigger_dag_id=HOURLY_DAG_ID,
        conf= {
            "source": "5mins_trigger",
            "is_full_refresh": "{{ ti.xcom_pull(task_ids='check_hourly_data_freshness')['is_full_refresh'] }}",
            "5mins_is_full_refresh": "{{ ti.xcom_pull(task_ids='check_hourly_data_freshness')['5mins_is_full_refresh'] }}",
            "5mins_start_date": "{{ ti.xcom_pull(task_ids='check_hourly_data_freshness')['5mins_start_date'] }}",
            "5mins_end_date": "{{ ti.xcom_pull(task_ids='check_hourly_data_freshness')['5mins_end_date'] }}"
        }, 
        wait_for_completion=False  # Don't wait for the hourly DAG to complete
    )
    
    # Set task dependencies
    start_task >> check_mode >> branch_to_extract_or_skip
    branch_to_extract_or_skip >> [extract_and_load_task, end_task]
    extract_and_load_task >> branch_to_transform_task
    branch_to_transform_task >> [transform_silver_5mins_incremental, transform_silver_full_refresh] >> check_hourly_freshness
    check_hourly_freshness >> branch_hourly_pipeline
    branch_hourly_pipeline >> [trigger_hourly_pipeline, end_task]
    trigger_hourly_pipeline >> end_task


# Instantiate the DAG
five_mins_processing_pipeline()