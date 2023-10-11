import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago
import os
import logging
import psycopg2



# Define your PostgreSQL connection ID from Airflow UI
postgres_conn_id = 'postgres_'

# Excel file paths
rent_collection_excel_path = os.path.join(os.path.dirname(__file__), 'data', 'rent_details.xlsx')
expenses_excel_path = os.path.join(os.path.dirname(__file__), 'data', 'expenses.xlsx')
logs_file_path = os.path.join(os.path.dirname(__file__),"logs")

def logger_func(log_file):

    # Configure logging to create a logger instance
    logger = logging.getLogger('ralopha_logger')
    logger.setLevel(logging.INFO)  
# Create a console handler and set the log level for it
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)  
# Create a file handler and set the log level for it
    file_handler = logging.FileHandler(log_file)  
    file_handler.setLevel(logging.INFO)
# Create a formatter and attach it to the handlers
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(formatter)
    file_handler.setFormatter(formatter)
# Attach the handlers to the logger
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)
    return logger

log_file = os.path.join(logs_file_path, f"ralopha_logs.log" )
my_logger = logger_func(log_file=log_file)

# Python function to extract data from Excel files
def extract_excel_data(file_path):
    df = pd.read_excel(file_path)
    return df

# Define the default_args for your DAG
default_args = {
    'owner': 'chandi',
    'depends_on_past': False,
    'start_date': days_ago(1),
    # 'retries': 1,
}

# Create the Airflow DAG
with DAG(
    'excel_to_postgres_etl',
    default_args=default_args,
    schedule_interval=None,  # Set your desired schedule interval
    catchup=False,
) as dag:
    
    # Task 1: Extract data from Excel files
    extract_rent_data = PythonOperator(
        task_id='extract_rent_data',
        python_callable=extract_excel_data,
        op_args=[rent_collection_excel_path],
    )
    
    extract_expenses_data = PythonOperator(
        task_id='extract_expenses_data',
        python_callable=extract_excel_data,
        op_args=[expenses_excel_path],
    )
    
    def rent_table():
        create_table_sql = """
            CREATE TABLE IF NOT EXISTS rent_collections (
                date_received DATE,
                house_no VARCHAR(255),
                tenant_name VARCHAR(255),
                tenant_phone INT,
                house_rent INT,
                rent_amount_paid INT,
                water_amount_paid INT,
                garbage_amount_paid INT,
                expenses_on_tenant INT,
                total_amount_paid INT
            );
            """
        Postgres_hook = PostgresHook(postgres_conn_id = postgres_conn_id)
        Postgres_hook.run(sql=create_table_sql)
        
    get_rent_data = PythonOperator(
    task_id="get_rent",python_callable=rent_table,dag=dag,
    )
    
    def expenses_table():
        create_expenses_sql = """
            CREATE TABLE IF NOT EXISTS expenses (
                date DATE,
                expense_category VARCHAR(255),
                expense_label VARCHAR(255),
                amount INT    
            );
            """
        Postgres_hook = PostgresHook(postgres_conn_id = postgres_conn_id)
        Postgres_hook.run(sql=create_expenses_sql)
    
    get_expenses_data = PythonOperator(
    task_id="get_expenses",python_callable=expenses_table,dag=dag,
    )
    
    # Task 3: Load data into PostgreSQL tables
    def load_rent_data():
        df = extract_excel_data(rent_collection_excel_path)
        hook = PostgresHook(postgres_conn_id = postgres_conn_id)
        conn = hook.get_conn()
        cur = conn.cursor()
        for index,row in df.iterrows():
            try:
                cur.execute("""
                INSERT INTO rent_collections (
                    date_received,
                    house_no,
                    tenant_name,
                    tenant_phone,
                    house_rent,
                    rent_amount_paid,
                    water_amount_paid,
                    garbage_amount_paid,
                    expenses_on_tenant,
                    total_amount_paid
                )
                VALUES (%s::date,%s,%s,%s::integer,%s::integer,%s::integer,%s::integer,%s::integer,%s::integer,%s::integer);
                """,
            (
            row['date_received'], row['house_no'], row['tenant_name'], row['tenant_phone'],
            row['house_rent'], row['rent_amount_paid'], row['water_amount_paid'],
            row['garbage_amount_paid'], row['expenses_on_tenant'], row['total_amount_paid']
            )
                )
                conn.commit()
            except psycopg2.DatabaseError as e:
                my_logger.error(f"Error while inserting row {index}: {str(e)}")

        
    load_rent = PythonOperator(
    task_id="load_rent",python_callable=load_rent_data,dag=dag,
    )
    
    def load_expense_data():
        df = extract_excel_data(expenses_excel_path)
        hook = PostgresHook(postgres_conn_id = postgres_conn_id)
        conn = hook.get_conn()
        cur = conn.cursor()
        for index,row in df.iterrows():
            try:
                cur.execute("""
                INSERT INTO expenses (
                            date,
                            expense_category, 
                            expense_label,
                            amount
                )
                VALUES (%s::date,%s,%s,%s::integer);
                """,
            (
            row['date'], row['expense_category'], row['expense_label'], row['amount']
            )
                )
                conn.commit()
            except psycopg2.DatabaseError as e:
                my_logger.error(f"Error while inserting row {index}: {str(e)}")
        
    load_expenses = PythonOperator(
    task_id="load_expenses",python_callable=load_expense_data,dag=dag,
    )
    
    # Define task dependencies
    extract_rent_data >> extract_expenses_data >> get_rent_data >> get_expenses_data >> load_rent >> load_expenses
