# PlayStudio Data Pipeline

## Project Overview

This repository contains a comprehensive data pipeline solution for processing and analyzing user activity data in PlayStudio games. The pipeline ingests data from source systems, processes it through bronze, silver, and gold layers, and makes it available for analytics and reporting.

## Architecture

The pipeline consists of:

1. **Airflow DAGs** - Orchestration layer responsible for:
   - 5-minute incremental processing of purchase data
   - Hourly processing of spins data
   - Triggering dependencies between pipelines

2. **DBT Models** - Transformation layer organized in:
   - Bronze Layer - Raw ingested data
   - Silver Layer - Cleansed, typed and deduplicated data
   - Gold Layer - Business-ready aggregated metrics

3. **PostgreSQL** - Data storage across:
   - Source database (app_source schema)
   - Data warehouse (bronze_layer, silver_layer, gold_layer schemas)

![Architecture Diagram](docs/architecture_diagram.png)

## Infrastructure Setup

### Prerequisites

- Docker and Docker Compose
- Git
- Python 3.8+
- PostgreSQL 13+

### Step 1: Clone the Repository

```bash
git clone https://github.com/your-org/playstudio-pipeline.git
cd playstudio-pipeline
```

### Step 2: Set Up Environment Variables

Create a `.env` file in the root directory:

```
# Database connections
WAREHOUSE_HOST=postgres
WAREHOUSE_USER=postgres
WAREHOUSE_PASSWORD=yourpassword

# Airflow settings
AIRFLOW_HOME=/opt/airflow
AIRFLOW_UID=50000
AIRFLOW_GID=0

# Application settings
OUTPUT_PATH=FINAL_TABLE.csv
DATA_SOURCE_PATH=data_source/MOC - Data.xlsx
GX_PROJECT_DIR=gx_project
GX_SUITE_NAME=aggregate_revenue_hourly_suite
GX_CHECKPOINT_NAME=data_quality_checkpoint
OPEN_DATA_DOCS=false
```

### Step 3: Initialize Infrastructure with Docker Compose

```bash
# Build the services
docker-compose build

# Initialize Airflow database
docker-compose up airflow-init

# Start all services
docker-compose up -d
```

### Step 4: Create Database Schemas

```bash
# Connect to PostgreSQL and create required schemas
docker-compose exec postgres psql -U postgres -c "
CREATE SCHEMA IF NOT EXISTS app_source;
CREATE SCHEMA IF NOT EXISTS bronze_layer;
CREATE SCHEMA IF NOT EXISTS silver_layer;
CREATE SCHEMA IF NOT EXISTS gold_layer;
"
```

### Step 5: Initialize DBT

```bash
# Navigate to DBT project directory
cd dbt/playstudio_project

# Install DBT dependencies
dbt deps

# Test connections
dbt debug

# Run initial models (optional)
dbt run --full-refresh
```

## Pipeline Components

### 1. 5-Minute Processing Pipeline

The `process_bronze_and_silver_layer` DAG runs every 5 minutes and:
- Extracts purchase data from source systems
- Loads data into the bronze layer
- Transforms data into the silver layer
- Checks freshness of hourly data
- Triggers the hourly pipeline when necessary

Key configurations:
- Supports incremental and full refresh modes
- Validates date ranges for backfills
- Ensures data quality through tests

### 2. Hourly Processing Pipeline

The `hourly_processing_pipeline` DAG processes hourly aggregated spin data and:
- Extracts spins data from source systems
- Loads data into the bronze layer
- Transforms data into silver and gold layers
- Creates final aggregated business metrics

### 3. Shared Utilities

Located in `utils/pipeline_utils.py`, these provide common functionality:
- Database connection management
- Data extraction and loading functions
- Date validation
- DBT command generation

## Data Models

### Bronze Layer Models

Raw data ingested from source systems:
- `bronze_purchases` - Raw purchase transaction data
- `bronze_spins_hourly` - Raw hourly spin activity data

### Silver Layer Models

Cleaned and standardized data:
- `silver_purchases` - Deduplicated purchase data with proper types
- `silver_spins_hourly` - Validated hourly spin data

### Gold Layer Models

Business-ready metrics:
- `aggregated_final_table` - Combined metrics including:
  - Hourly spin counts
  - Purchase amounts and revenue
  - Daily revenue aggregations
  - User and country dimensions

## Data Quality Framework

The pipeline implements extensive data quality checks:

1. **Column-level validations**:
   - Type checking
   - Non-null constraints
   - Value range validations
   - Format validations (regex patterns)

2. **Table-level tests**:
   - Row count validations
   - Uniqueness constraints
   - Referential integrity

3. **Business logic validations**:
   - Metric relationship validations
   - Aggregation consistency
   - Temporal validations

4. **Freshness checks**:
   - Silver data freshness (10-30 minutes)
   - Hourly data freshness (1-3 hours)
   - Daily aggregation completeness

## Usage

### Running the Pipelines

1. **Manual Triggering**:
   - Access Airflow UI at http://localhost:8080
   - Login with your credentials
   - Navigate to DAGs
   - Trigger `process_bronze_and_silver_layer` or `hourly_processing_pipeline`

2. **Backfill Processing**:
   - Set DAG run configuration parameters:
     ```json
     {
       "is_full_refresh": true,
       "backfill_start_date": "2023-01-01 00:00:00",
       "backfill_end_date": "2023-01-31 23:59:59"
     }
     ```

### Monitoring & Troubleshooting

1. **Check DAG Logs**:
   - Access Airflow UI
   - Navigate to DAG > Graph View
   - Click on specific task and select "Logs"

2. **Verify DBT Tests**:
   ```bash
   cd dbt/playstudio_project
   dbt test
   ```

3. **Data Quality Reporting**:
   - Check test failures in Airflow logs
   - Review DBT test results

## Maintenance

### Adding New Source Data

1. Update the relevant source configuration in:
   - `dbt/playstudio_project/models/sources.yml`
   - `utils/pipeline_utils.py` DB_CONFIG dictionary

2. Create new bronze and silver models

3. Update the gold layer aggregations

### Updating Dependencies

```bash
# Update DBT packages
cd dbt/playstudio_project
dbt deps

# Update Python dependencies
pip install --upgrade -r requirements.txt
```

## Project Structure

```
.
├── dags/
│   ├── 5mins_processing_pipeline.py
│   ├── hourly_processing_pipeline.py
│   └── utils/
│       └── pipeline_utils.py
        └── slack_alert.py
├── dbt/
│   └── playstudio_project/
│       ├── models/
│       │   ├── bronze_layer/
│       │   ├── silver_layer/
│       │   │   ├── silver_purchases.sql
│       │   │   ├── silver_purchases.yml
│       │   │   ├── silver_spins_hourly.sql
│       │   │   └── silver_spins_hourly.yml
│       │   └── gold_layer/
│       │       ├── aggregated_final_table.sql
│       │       └── aggregated_final_table.yml
│       ├── packages.yml
│       └── dbt_project.yml
├── docker-compose.yml
├── .env.example
└── README.md
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Run tests to ensure quality
5. Submit a pull request

## License

This project is licensed under the [MIT License](LICENSE).
