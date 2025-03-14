# PlayStudio Data Pipeline

A comprehensive data processing and analysis pipeline for PlayStudio user activity data. This project processes purchase and spins data at different intervals, validates data quality, and creates aggregated views for business insights.

## Project Overview

This project implements a data engineering solution that satisfies the following requirements:

1. **Task #1**: Process raw purchases and spins data from source files into a structured table with clear column naming
2. **Task #2**: Implement data validation tests to ensure data quality and integrity
3. **Task #3**: Create an Airflow pipeline architecture that handles:
   - Purchase data (updated every 5 minutes)
   - Spins hourly data (updated hourly)
   - Merging both data sources into a final aggregated table

The solution follows a medallion architecture with bronze (raw), silver (processed), and gold (aggregated) data layers.

## Architecture

![Architecture Diagram](docs/architecture_diagram.png)

The project consists of:

- **Data Sources**: Excel files with raw purchase and spins data
- **Data Processing**:
  - Python scripts to extract and transform data
  - Great Expectations for data validation
- **Data Storage**: PostgreSQL database with dedicated schemas for each data layer
- **Orchestration**: Airflow DAGs that run at 5-minute and hourly intervals
- **Transformation**: DBT models that implement data quality checks and business logic

## Infrastructure Setup

### Prerequisites

- Docker and Docker Compose
- Python 3.8 or higher
- Git

### Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/yourusername/playstudio-data-pipeline.git
   cd playstudio-data-pipeline
   ```

2. Set up environment variables (or use the provided .env file):
   ```bash
   # Database connections
   DB_HOST=localhost
   DB_PORT=5432
   DB_USER=airflow
   DB_PASSWORD=airflow
   
   # Data source configuration
   DATA_SOURCE_PATH=scripts/data_source/MOC.xlsx
   ```

3. Build and start the infrastructure:
   ```bash
   cd pipelines
   docker-compose build
   docker-compose up -d
   ```

4. Initialize the database schemas:
   ```bash
   cd ..
   python scripts/prepare_env.py
   ```

5. Access Airflow UI at http://localhost:8080 (username: airflow, password: airflow)

## Pipeline Components

### 5-Minute Processing Pipeline

The `five_mins_processing_pipeline.py` DAG:
- Runs every 5 minutes
- Extracts new purchase data from the source
- Loads it into the bronze layer
- Transforms and validates data in the silver layer
- Can trigger the hourly processing pipeline when needed

Key features:
- Incremental processing to handle only new data
- Data quality checks at ingestion
- Error handling and retry mechanisms

### Hourly Processing Pipeline

The `hourly_processing_pipeline.py` DAG:
- Runs hourly (or when triggered by the 5-minute pipeline)
- Processes spins hourly data into the bronze and silver layers
- Merges purchase and spins data into the gold layer aggregated table
- Applies business rules and calculates metrics

### Shared Utilities

The `pipeline_utils.py` provides common functionality:
- Database connection handling
- Data extraction and loading functions
- Validation utilities
- DBT command generation

## Data Models

### Bronze Layer
- Raw data with minimal transformations
- Preserves the original data structure
- Includes metadata such as extraction timestamps

### Silver Layer
- `silver_purchases`: Validated and cleaned purchase transactions
  - Deduplication based on transaction_id
  - Type conversions and standardization
  - Basic validations like non-null checks

- `silver_spins_hourly`: Validated and cleaned hourly spins data
  - Aggregated by hour, user, and country
  - Type conversions and standardization

### Gold Layer
- `aggregated_final_table`: Business-level aggregated metrics
  - Combines purchases and spins data
  - Provides hourly and daily metrics per user and country
  - Includes calculations like total revenue, average revenue per purchase, etc.

## Data Quality Framework

Data validation happens at multiple levels:

1. **Source data validation** (scripts/task1_task2.py):
   - Great Expectations framework to validate incoming data
   - Schema validation, type checking, and business rule validation

2. **Silver layer validation** (DBT models):
   - Column-level tests (not_null, unique, data types)
   - Relationship tests between tables
   - Value validation (e.g., revenue must be positive)
   - Freshness checks to ensure data is current

3. **Gold layer validation** (DBT models):
   - Business rule validation
   - Cross-table consistency checks
   - Aggregation integrity tests
   - Statistical distribution checks

When validation fails:
- Critical issues stop the pipeline and alert via Slack
- Warnings are logged for review but allow the pipeline to continue
- All issues are documented in logs and DBT test results

## Usage

### Running the Pipelines

The pipelines are scheduled to run automatically, but you can also trigger them manually:

1. 5-minute pipeline:
   ```bash
   airflow dags trigger five_mins_processing_pipeline
   ```

2. Hourly pipeline:
   ```bash
   airflow dags trigger hourly_processing_pipeline
   ```

### Running Backfills

To process historical data:

```bash
airflow dags backfill \
  --start-date 2023-01-01 \
  --end-date 2023-01-02 \
  five_mins_processing_pipeline
```

### Running DBT Models Manually

```bash
cd pipelines/dbt/playstudio_project
dbt run --select silver_purchases silver_spins_hourly
dbt run --select aggregated_final_table
dbt test
```

## Monitoring and Troubleshooting

- **Airflow UI**: Check task status and logs
- **DBT documentation**: View data lineage and test results
- **Database inspection**: Query the tables directly to verify data

Common issues and resolutions:
- Database connection errors: Check environment variables and network connectivity
- Data validation failures: Inspect the source data for anomalies
- Airflow scheduling issues: Check for DAG conflicts or resource constraints

## Maintenance

### Adding New Data Sources

1. Update the source extraction logic in `pipeline_utils.py`
2. Create new bronze and silver models in DBT
3. Modify the gold layer model to incorporate the new data
4. Add appropriate tests for data validation

### Updating Dependencies

The project dependencies are specified in:
- `requirements.txt` for Python packages
- `packages.yml` for DBT packages

## Project Structure

```
playstudio-data-pipeline/
├── scripts/
│   ├── data_source/
│   │   └── MOC.xlsx              # Source data
│   ├── utils/
│   │   └── excel_utils.py        # Excel processing utilities
│   ├── task1_task2.py            # Initial data processing script
│   └── prepare_env.py            # Environment setup script
│
├── pipelines/
│   ├── dags/
│   │   ├── five_mins_processing_pipeline.py  # 5-minute DAG
│   │   ├── hourly_processing_pipeline.py     # Hourly DAG
│   │   └── utils/
│   │       ├── pipeline_utils.py             # Shared utilities
│   │       └── slack_alert.py                # Alerting functionality
│   │
│   ├── dbt/
│   │   └── playstudio_project/
│   │       ├── models/
│   │       │   ├── silver_layer/
│   │       │   │   ├── silver_purchases.sql
│   │       │   │   ├── silver_purchases.yml  # Data quality tests
│   │       │   │   ├── silver_spins_hourly.sql
│   │       │   │   └── silver_spins_hourly.yml
│   │       │   │
│   │       │   └── gold_layer/
│   │       │       ├── aggregated_final_table.sql
│   │       │       └── aggregated_final_table.yml
│   │       │
│   │       ├── dbt_project.yml
│   │       └── packages.yml
│   │
│   ├── docker-compose.yaml       # Infrastructure definition
│   ├── Dockerfile                # Airflow container setup
│   └── requirements.txt          # Python dependencies
│
└── .env                          # Environment variables
```

## Contributing

1. Fork the repository
2. Create a feature branch: `git checkout -b feature-name`
3. Commit your changes: `git commit -m 'Add feature'`
4. Push to the branch: `git push origin feature-name`
5. Submit a pull request

## License

This project is licensed under the MIT License - see the LICENSE file for details.
