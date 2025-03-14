import os
import pandas as pd
import numpy as np
import great_expectations as ge
from datetime import datetime
from dotenv import load_dotenv
from pathlib import Path
from utils.excel_utils import extract_value_after_equals, read_excel_file
from great_expectations.data_context import FileDataContext

# Load environment variables
load_dotenv()

def preprocess_spins_data(spins_df):
    """Preprocess the spins data."""
    # Convert dates and validate
    spins_df['DATE'] = pd.to_datetime(spins_df['DATE'], errors='coerce')
    if spins_df['DATE'].isna().any():
        raise ValueError("Invalid timestamp format detected in spins data")
    # Floor timestamps to hourly
    spins_df['APP_HOUR'] = spins_df['DATE'].dt.floor('H')
    
    # Aggregate spins data
    spins_agg = spins_df.groupby(['APP_HOUR', 'USERID', 'COUNTRY']).agg(
        TOTAL_SPINS_HOURLY=('TOTAL_SPINS', 'sum')
    ).reset_index()


    spins_agg['TOTAL_SPINS_HOURLY'] = np.ceil(spins_agg['TOTAL_SPINS_HOURLY'].fillna(0)).astype(int)


    return spins_agg

def preprocess_purchases_data(purchases_df):
    """Preprocess the purchases data."""
    # Extract revenue values
    purchases_df['REVENUE'] = purchases_df['REVENUE'].apply(extract_value_after_equals)
    if purchases_df['REVENUE'].isna().any():
        print("Warning: Some REVENUE values were invalid and set to NaN:", 
              purchases_df[purchases_df['REVENUE'].isna()][['DATE', 'USERID', 'TRANSACTION_ID']])
    
    # Convert dates and validate
    purchases_df['DATE'] = pd.to_datetime(purchases_df['DATE'], errors='coerce')
    if purchases_df['DATE'].isna().any():
        raise ValueError("Invalid timestamp format detected in purchases data")
    
    # Floor timestamps to hourly
    purchases_df['APP_HOUR'] = purchases_df['DATE'].dt.floor('H')
    
    # Remove duplicates
    purchases_df = purchases_df.drop_duplicates(subset=['TRANSACTION_ID'])
    
    # Aggregate purchases data
    purchases_agg = purchases_df.groupby(['APP_HOUR', 'USERID']).agg(
        TOTAL_PURCHASES_AMOUNT_HOURLY=('TRANSACTION_ID', 'count'),
        TOTAL_REVENUE_HOURLY=('REVENUE', 'sum')
    ).reset_index()
    
    return purchases_agg

def merge_and_calculate_metrics(spins_agg, purchases_agg):
    """Merge datasets using an outer join and calculate derived metrics."""
    # Merge datasets using an outer join
    merged_df = spins_agg.merge(purchases_agg, on=['APP_HOUR', 'USERID'], how='outer')
    
    # Fill missing values with defaults
    merged_df['APP_HOUR'] = merged_df['APP_HOUR'].fillna(pd.Timestamp('1970-01-01'))
    merged_df['USERID'] = merged_df['USERID'].fillna('UNKNOWN')
    merged_df['COUNTRY'] = merged_df['COUNTRY'].fillna('UNKNOWN')
    merged_df['TOTAL_SPINS_HOURLY'] = merged_df['TOTAL_SPINS_HOURLY'].fillna(0).astype(int)
    merged_df['TOTAL_PURCHASES_AMOUNT_HOURLY'] = merged_df['TOTAL_PURCHASES_AMOUNT_HOURLY'].fillna(0).astype(int)
    merged_df['TOTAL_REVENUE_HOURLY'] = merged_df['TOTAL_REVENUE_HOURLY'].fillna(0).astype(float)
    
    # Calculate derived metrics
    merged_df['AVG_REVENUE_PER_PURCHASE_HOURLY'] = (
        merged_df['TOTAL_REVENUE_HOURLY'] / merged_df['TOTAL_PURCHASES_AMOUNT_HOURLY']
    ).fillna(0).astype(float)

    # Calculate daily revenue per user
    merged_df['DATE'] = merged_df['APP_HOUR'].dt.date
    daily_revenue = merged_df.groupby(['DATE', 'USERID']).agg(
        TOTAL_REVENUE_PER_USER_DAILY=('TOTAL_REVENUE_HOURLY', 'sum')
    ).reset_index()
    final_df = merged_df.merge(daily_revenue, on=['DATE', 'USERID'], how='left')
    final_df['TOTAL_REVENUE_PER_USER_DAILY'] = final_df['TOTAL_REVENUE_PER_USER_DAILY'].fillna(0).astype(float)

    # Select final columns
    final_df = final_df[[
        'APP_HOUR', 'USERID', 'COUNTRY', 'TOTAL_SPINS_HOURLY', 
        'TOTAL_REVENUE_HOURLY', 'TOTAL_PURCHASES_AMOUNT_HOURLY', 
        'AVG_REVENUE_PER_PURCHASE_HOURLY', 'TOTAL_REVENUE_PER_USER_DAILY'
    ]]
    
    return final_df

def create_table(data_source_path):
    """Create a processed table from Excel data sources."""
    # Load the Excel data
    spins_df = read_excel_file(
        file_path=data_source_path,
        sheet_name='Spins Hourly',
        usecols=['date', 'userId', 'country', 'total_spins']
    )
    
    purchases_df = read_excel_file(
        file_path=data_source_path,
        sheet_name='Purchases',
        usecols=['date', 'userId', 'revenue', 'transaction_id']
    )

    # Process each dataset separately
    spins_agg = preprocess_spins_data(spins_df)
    purchases_agg = preprocess_purchases_data(purchases_df)
    
    # Merge and calculate metrics
    final_df = merge_and_calculate_metrics(spins_agg, purchases_agg)
 
    # Get output path from environment variable or use default
    output_path = os.getenv('OUTPUT_PATH', 'FINAL_TABLE.csv')
    # Sort by APP_HOUR before saving
    final_df = final_df.sort_values(by='APP_HOUR')
    # Save to CSV
    final_df.to_csv(output_path, index=False)
    print(f"Task #1 complete: {output_path} created")
    return final_df

def setup_expectations_suite(context, suite_name):
    """Set up and configure the Great Expectations suite."""
    # Initialize data context
    data_source = context.data_sources.add_pandas(name="pandas")
    data_asset = data_source.add_dataframe_asset(name="pd_dataframe_asset")
    batch_definition = data_asset.add_batch_definition_whole_dataframe("batch_definition")

    suite = ge.ExpectationSuite(name=suite_name)
    suite = context.suites.add(suite)
    
    validation_definition = ge.ValidationDefinition(
        data=batch_definition, 
        suite=suite, 
        name='data_validation'
    )
    
    return suite, validation_definition, batch_definition

def add_data_quality_expectations(suite, df):
    """Add data quality expectations to the suite."""
    # 1. Completeness
    for col in ['APP_HOUR', 'USERID', 'COUNTRY']:
        suite.add_expectation(ge.expectations.ExpectColumnValuesToNotBeNull(column=col))

    # 2. Non-Negative Values
    for col in ['TOTAL_SPINS_HOURLY', 'TOTAL_REVENUE_HOURLY', 'TOTAL_PURCHASES_AMOUNT_HOURLY', 
                'AVG_REVENUE_PER_PURCHASE_HOURLY', 'TOTAL_REVENUE_PER_USER_DAILY']:
        suite.add_expectation(ge.expectations.ExpectColumnValuesToBeBetween(column=col, min_value=0))

    # 3. Data Type Consistency
    suite.add_expectation(ge.expectations.ExpectColumnValuesToBeOfType(column='USERID', type_='str'))
    suite.add_expectation(ge.expectations.ExpectColumnValuesToBeOfType(column='COUNTRY', type_='str'))
    suite.add_expectation(ge.expectations.ExpectColumnValuesToBeOfType(column='TOTAL_SPINS_HOURLY', type_='int64'))
    suite.add_expectation(ge.expectations.ExpectColumnValuesToBeOfType(column='TOTAL_PURCHASES_AMOUNT_HOURLY', type_='int64'))
    suite.add_expectation(ge.expectations.ExpectColumnValuesToBeOfType(column='TOTAL_REVENUE_HOURLY', type_='float64'))
    suite.add_expectation(ge.expectations.ExpectColumnValuesToBeOfType(column='AVG_REVENUE_PER_PURCHASE_HOURLY', type_='float64'))
    suite.add_expectation(ge.expectations.ExpectColumnValuesToBeOfType(column='TOTAL_REVENUE_PER_USER_DAILY', type_='float64'))

    # 4. Reasonable Ranges
    suite.add_expectation(ge.expectations.ExpectColumnValuesToBeBetween(column='TOTAL_SPINS_HOURLY', max_value=10000))
    suite.add_expectation(ge.expectations.ExpectColumnValuesToBeBetween(column='TOTAL_PURCHASES_AMOUNT_HOURLY', max_value=1000))

    # 5. Row count validation
    suite.add_expectation(ge.expectations.ExpectTableRowCountToEqual(
        value=int(df.shape[0])
    ))
    
    return suite

def validate_table(df):
    """Validate the table using Great Expectations."""
    # Get GX project directory from environment variable or use default
    gx_project_dir = os.getenv('GX_PROJECT_DIR', 'gx_project')
    suite_name = os.getenv('GX_SUITE_NAME', 'aggregate_revenue_hourly_suite')
    checkpoint_name = f"{os.getenv('GX_CHECKPOINT_NAME', 'data_quality_checkpoint')}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    
    # Create directory if it doesn't exist
    # Path(gx_project_dir).mkdir(exist_ok=True)
    
    # Initialize context
    context = FileDataContext._create(project_root_dir=gx_project_dir)

    context = ge.get_context()
    
    # Setup expectations suite
    suite, validation_definition, batch_definition = setup_expectations_suite(context, suite_name)
    
    # Add data quality expectations
    suite = add_data_quality_expectations(suite, df)
    
    # Run validation
    try:
        validation_definition = context.validation_definitions.add(validation_definition)
    except:
        validation_definition = context.validation_definitions.get('data_validation')
    
    batch_parameters = {"dataframe": df}
    validation_results = validation_definition.run(batch_parameters=batch_parameters)
    
    # Set up checkpoint with actions
    checkpoint = ge.Checkpoint(
        name=checkpoint_name,
        validation_definitions=[validation_definition],
        actions=[ge.checkpoint.UpdateDataDocsAction(name="update_data_docs")],
        result_format={"result_format": "COMPLETE"},
    )
    
    context.checkpoints.add(checkpoint)
    checkpoint_results = checkpoint.run(batch_parameters=batch_parameters)
    
    # Generate documentation
    context.build_data_docs()
    
    # Open data docs only if environment variable is set to true
    if os.getenv('OPEN_DATA_DOCS', 'false').lower() == 'true':
        context.open_data_docs()
    
    return validation_results

if __name__ == "__main__":
    # Get data source path from environment variable or use default
    data_source_path = os.getenv('DATA_SOURCE_PATH', 'data_source/MOC - Data.xlsx')
    
    final_df = create_table(data_source_path)
    validation_results = validate_table(final_df)
    print("Data processing and validation completed.")