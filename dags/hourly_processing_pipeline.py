"""
DAG for processing hourly data into bronze, silver, and gold layers.
Can be triggered manually or by the 5-minute processing pipeline.
"""
# Standard library imports
import logging
from datetime import datetime
from typing import Dict, Any

# Airflow imports
from airflow.decorators import task, dag
from airflow.operators.python import BranchPythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.trigger_rule import TriggerRule

# Import shared utilities
from utils.pipeline_utils import (
    DB_CONFIG, DEFAULT_ARGS, INITIAL_INCREMENTAL_TIME,
    validate_datetime, extract_data_from_source, get_latest_updated_at_from_table,
    load_data_to_warehouse, get_dbt_command, on_failure_callback
)


# Set up logger
logger = logging.getLogger(__name__)

@dag(
    dag_id="hourly_processing_pipeline",
    schedule_interval=None,  # This DAG will be triggered by another DAG
    start_date=days_ago(1),
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    catchup=False,
    params={
        "source": "Self Trigger",
        "is_full_refresh": "True",
        "backfill_start_date": None,
        "backfill_end_date": None,
        "template_date": "2025-03-12 00:00:00"
    },
    tags=["incremental_ingestion"],
    is_paused_upon_creation=False,
    on_failure_callback=on_failure_callback
)
def hourly_processing_pipeline():
    """
    DAG for ingesting data into the bronze layer with configurable incremental or full refresh modes.
    This DAG can be triggered by another DAG with configuration parameters.
    
    This DAG supports two modes:
    1. Incremental loading: Processes only new data since the last run
    2. Full refresh: Processes all data within a specified date range
    """
    
    @task(task_id="check_refresh_mode_and_validate")
    def check_refresh_mode_and_validate(**context) -> Dict[str, Any]:
        """
        Check if we're running in full refresh or incremental mode and
        validate date ranges for full refresh mode.
        
        Returns:
            Dict[str, Any]: Dictionary containing validation results and mode information
        """
        # Get parameters either from trigger conf or from DAG params
        conf = context.get('params', {})
        logger.info(f"Received configuration: {conf}")
        
        # Determine refresh mode - handle string value from JSON
        source = conf.get('source', 'Self Trigger')
        if source == "Self Trigger":
            is_full_refresh = str(conf.get('is_full_refresh', "True")).lower() == "true"
            logger.info(f"Using Self Trigger conf: is_full_refresh={is_full_refresh}")
        else:
            is_full_refresh = str(conf.get('is_full_refresh', "False")).lower() == "true"
            logger.info(f"Using external trigger conf: is_full_refresh={is_full_refresh}")
        
        logger.info(f"Refresh mode: {'Full Refresh' if is_full_refresh else 'Incremental'}")
        
        # Check for first run condition
        warehouse_hook = PostgresHook(postgres_conn_id=DB_CONFIG["connections"]["warehouse"])
        latest_updated_at = get_latest_updated_at_from_table(
            warehouse_hook, 
            table_name=DB_CONFIG["bronze"]["tables"]["spins_hourly"], 
            schema_name=DB_CONFIG["bronze"]["schema"]
        )
        
        # If this is the first run with no data, force full refresh
        is_first_run = latest_updated_at == INITIAL_INCREMENTAL_TIME
        if is_first_run:
            logger.info("First run detected - using full refresh mode")
            is_full_refresh = True
        
        # Initialize validation results
        validation_result = {
            "is_full_refresh": is_full_refresh,
            "latest_updated_at": latest_updated_at,
            "is_first_run": is_first_run,
            "is_valid": True  # Default to valid
        }
        
        # Only validate dates if we're in full refresh mode and it's not the first run
        if is_full_refresh and not is_first_run:
            # Get date parameters based on trigger source
            if source == "Self Trigger":
                backfill_start_date = conf.get('backfill_start_date')
                backfill_end_date = conf.get('backfill_end_date')
                logger.info(f"Using Self Trigger conf for date range validation")
            elif source == "5mins_trigger" and str(conf.get('5mins_is_full_refresh', "False")).lower() == "true":
                backfill_start_date = conf.get('5mins_start_date')
                backfill_end_date = conf.get('5mins_end_date')
                logger.info(f"Using 5mins trigger conf for date range validation")
            else:
                backfill_start_date = None
                backfill_end_date = None
            
            logger.info(f"Full refresh mode - date range: {backfill_start_date} to {backfill_end_date}")
            
            # Check if dates are provided
            if backfill_start_date is None or backfill_end_date is None:
                logger.error("Error: backfill_start_date and backfill_end_date must be provided for full refresh")
                validation_result["is_valid"] = False
                return validation_result
            
            # Validate date format and range
            try:
                start_date = validate_datetime(backfill_start_date)
                end_date = validate_datetime(backfill_end_date)
                
                # Check if start date is before end date
                if start_date > end_date:
                    logger.error(f"Invalid date range: start date {start_date} is after end date {end_date}")
                    validation_result["is_valid"] = False
                else:
                    logger.info(f"Validated date range: {start_date} to {end_date}")
                    validation_result["start_date"] = backfill_start_date
                    validation_result["end_date"] = backfill_end_date
            except ValueError as e:
                logger.error(f"Date validation error: {e}")
                validation_result["is_valid"] = False
        
        return validation_result
    
    def decide_extract_task(**context) -> str:
        """
        Decide whether to proceed with extraction or skip
        
        Returns:
            Task ID to execute next
        """
        ti = context['ti']
        
        # Get combined validation and refresh mode info
        validation_info = ti.xcom_pull(task_ids="check_refresh_mode_and_validate")
        
        # Check validation status
        if not validation_info["is_valid"]:
            logger.info("Skipping extraction due to invalid parameters")
            return "end_dag"
        
        logger.info("Proceeding with extraction")
        return "extract_and_load_spins_hourly"
    
    def decide_transform_task(**context) -> str:
        """
        Decide which transform task to run based on refresh mode
        
        Returns:
            Task ID of the transform task to execute
        """
        ti = context['ti']
        validation_info = ti.xcom_pull(task_ids="check_refresh_mode_and_validate")
        
        if validation_info["is_full_refresh"]:
            logger.info("Selected transform task: transform_silver_full_refresh")
            return "transform_silver_full_refresh"
        else:
            logger.info("Selected transform task: transform_silver_hourly_incremental")
            return "transform_silver_hourly_incremental"
    
    @task(task_id="extract_and_load_spins_hourly")
    def extract_and_load_spins_hourly(**context) -> str:
        """
        Extract spins data from PostgreSQL and load it into the Bronze Layer.
        
        Returns:
            Result message with number of rows inserted
        """
        # Initialize task instance and database connections
        ti = context['ti']
        pg_source_hook = PostgresHook(postgres_conn_id=DB_CONFIG["connections"]["source"])
        warehouse_hook = PostgresHook(postgres_conn_id=DB_CONFIG["connections"]["warehouse"])
        
        # Get refresh mode information
        validation_info = ti.xcom_pull(task_ids="check_refresh_mode_and_validate")
        is_full_refresh = validation_info["is_full_refresh"]
        logger.info(f"Running extract_and_load_spins_hourly in {'full refresh' if is_full_refresh else 'incremental'} mode")
        
        # Build the WHERE condition based on refresh type
        if is_full_refresh:
            if validation_info["is_first_run"]:
                # First run with no data - get all data
                condition = "1=1"
                logger.info("First run - fetching all data")
            else:
                # Backfill mode: Use provided date range
                backfill_start_date = validation_info["start_date"]
                backfill_end_date = validation_info["end_date"]
                condition = f"timestamp >= '{backfill_start_date}' AND timestamp <= '{backfill_end_date}'"
                logger.info(f"Full refresh with date range: {backfill_start_date} to {backfill_end_date}")
        else:
            # Incremental mode: Use last processed timestamp
            incremental_time = validation_info["latest_updated_at"]
            ti.xcom_push(key="incremental_time_processing", value=incremental_time)
            condition = f"timestamp > '{incremental_time}'"
            logger.info(f"Incremental mode - fetching data updated after: {incremental_time}")

        # Build and execute the query
        raw_schema = DB_CONFIG["raw"]["schema"]
        raw_table = DB_CONFIG["raw"]["tables"]["spins_hourly"]
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
            bronze_table = DB_CONFIG["bronze"]["tables"]["spins_hourly"]
            
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
            logger.error(f"Error in extract_and_load_spins_hourly: {str(e)}")
            raise
    
    # Define task instances
    
    # Start and end markers
    start_task = DummyOperator(task_id='start_dag')  
    end_task = DummyOperator(
        task_id='end_dag',
        trigger_rule=TriggerRule.ALL_DONE  # Ensure this task runs regardless of upstream success/failure
    )
    
    # Task flow steps
    check_mode_and_validate = check_refresh_mode_and_validate()
    
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
    transform_silver_hourly_incremental = BashOperator(
        task_id="transform_silver_hourly_incremental",
        bash_command=get_dbt_command(tag="silver_hourly_incremental", full_refresh=False)
    )
    
    transform_silver_full_refresh = BashOperator(
        task_id="transform_silver_full_refresh",
        bash_command=get_dbt_command(tag="silver_hourly_incremental", full_refresh=True)
    )
    
    # Gold layer aggregation tasks
    aggregate_final_table_incremental = BashOperator(
        task_id="aggregate_final_table_incremental",
        trigger_rule=TriggerRule.ONE_DONE,
        bash_command=get_dbt_command(tag="gold_layer", full_refresh=False)
    )
    
    aggregate_final_table_full_refresh = BashOperator(
        task_id="aggregate_final_table_full_refresh",
        trigger_rule=TriggerRule.ONE_DONE,
        bash_command=get_dbt_command(tag="gold_layer", full_refresh=True)
    )
    
    # Define main data fetching task
    extract_and_load_task = extract_and_load_spins_hourly()
    
    # Set task dependencies
    start_task >> check_mode_and_validate >> branch_to_extract_or_skip
    branch_to_extract_or_skip >> [extract_and_load_task, end_task]
    extract_and_load_task >> branch_to_transform_task
    branch_to_transform_task >> [transform_silver_hourly_incremental, transform_silver_full_refresh]
    transform_silver_hourly_incremental >> aggregate_final_table_incremental >> end_task
    transform_silver_full_refresh >> aggregate_final_table_full_refresh >> end_task


# Instantiate the DAG
hourly_processing_pipeline()