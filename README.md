# Excel to PostgreSQL ETL with Apache Airflow

This Apache Airflow DAG (Directed Acyclic Graph) is designed to perform an ETL process, extracting data from Excel files and loading it into PostgreSQL tables.

## Overview

The ETL process consists of the following tasks:

1. **Task 1**: Extract data from Rent Collection Excel file.
2. **Task 2**: Extract data from Expenses Excel file.
3. **Task 3**: Create Rent Collections and Expenses tables if they don't exist.
4. **Task 4**: Load Rent Collection data into the PostgreSQL database.
5. **Task 5**: Load Expenses data into the PostgreSQL database.

## Prerequisites

Before executing the DAG, ensure the following:

- PostgreSQL connection is set up in Airflow UI with the ID specified in the script (`postgres_conn_id`).
- Excel files (`rent_details.ods` and `expenses.ods`) exist in the specified paths.
- Airflow environment is properly configured.

## File Structure

- `excel_to_postgres_etl.py`: Airflow DAG script.
- `logs/`: Directory to store logs.
- `datasets/`: Directory containing Excel files.

## Logging

Logs are generated during the ETL process and stored in the `logs/` directory. The log file (`ralopha_logs.log`) provides insights into task execution and any encountered errors.

## Usage

1. Copy the `excel_to_postgres_etl.py` script to your Airflow DAGs directory.
2. Ensure the required Python packages (`pandas`, `psycopg2`, etc.) are installed in your Airflow environment.
3. Configure the PostgreSQL connection in the Airflow UI.
4. Run the DAG in the Airflow UI or trigger it using other Airflow mechanisms.

## Customization

- Adjust the file paths (`rent_collection_excel_path`, `expenses_excel_path`) if your Excel files are located in different directories.
- Modify the PostgreSQL connection ID (`postgres_conn_id`) to match the one configured in your Airflow environment.
- Set the desired schedule interval in the DAG constructor (currently commented out).

## Error Handling

The script includes error handling mechanisms to capture and log any issues during data extraction, table creation, and data loading into PostgreSQL.

## Notes

- The DAG is set to run daily (`start_date` and `schedule_interval` can be adjusted).
- Tables are created if they don't exist, ensuring a dynamic and flexible ETL process.
