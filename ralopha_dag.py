import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago
import os
import logging
import psycopg2
from datetime import timedelta




# Define your PostgreSQL connection ID from Airflow UI
postgres_conn_id = 'postgres_conn'

# Excel file paths
rent_collection_excel_path = '/home/chandi/Ralopha/datasets/rent_details.ods'
expenses_excel_path = '/home/chandi/Ralopha/datasets/expenses.ods'
logs_file_path = os.path.join(os.path.dirname(__file__),"logs")

def logger_func(log_file):

    logger = logging.getLogger('ralopha_logger')
    logger.setLevel(logging.INFO)  
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)  
    file_handler = logging.FileHandler(log_file)  
    file_handler.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(formatter)
    file_handler.setFormatter(formatter)
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
    # schedule_interval= timedelta(minutes=3),  # Set your desired schedule interval
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
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                id INT,
                date_received DATE,
                house_no VARCHAR(255),
                tenant_name VARCHAR(255),
                tenant_phone VARCHAR(255),
                house_rent INT,
                rent_amount_paid INT,
                water_amount_paid INT,
                garbage_amount_paid INT,
                expenses_on_tenant INT,
                total_amount_paid INT,
                CONSTRAINT primary_key_constraint UNIQUE (id)
            );
            """
        Postgres_hook = PostgresHook(postgres_conn_id = postgres_conn_id)
        Postgres_hook.run(sql=create_table_sql)
        
    get_rent_data = PythonOperator(
    task_id="get_rent",
    python_callable=rent_table,
    dag=dag,
    )
    
    def expenses_table():
        create_expenses_sql = """
            CREATE TABLE IF NOT EXISTS expenses (
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                table_id INT,
                date DATE,
                expense_category VARCHAR(255),
                expense_label VARCHAR(255),
                Amount INT,
                CONSTRAINT primary_key_constraint2 UNIQUE (table_id)
            );
            """
        Postgres_hook = PostgresHook(postgres_conn_id = postgres_conn_id)
        Postgres_hook.run(sql=create_expenses_sql)
    
    get_expenses_data = PythonOperator(
    task_id="get_expenses",python_callable=expenses_table,dag=dag,
    )
    
    # Task 3: Load data into PostgreSQL tables
    def load_rent_data(handle_duplicates=True):
        df = extract_excel_data(rent_collection_excel_path)
        hook = PostgresHook(postgres_conn_id = postgres_conn_id)
        conn = hook.get_conn()
        cur = conn.cursor()
        for index,row in df.iterrows():
            try:
                cur.execute("""
                INSERT INTO rent_collections (
                    id,        
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
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (id) DO UPDATE 
                SET
                    id = EXCLUDED.id,
                    date_received = EXCLUDED.date_received,
                    house_no = EXCLUDED.house_no,
                    tenant_name = EXCLUDED.tenant_name,
                    tenant_phone = EXCLUDED.tenant_phone,
                    house_rent = EXCLUDED.house_rent,
                    rent_amount_paid = EXCLUDED.rent_amount_paid,
                    water_amount_paid = EXCLUDED.water_amount_paid,
                    garbage_amount_paid = EXCLUDED.garbage_amount_paid,
                    expenses_on_tenant = EXCLUDED.expenses_on_tenant,
                    total_amount_paid = EXCLUDED.total_amount_paid;
                """,
            (
            row['id'],row['date_received'], row['house_no'], row['tenant_name'], row['tenant_phone'],
            row['house_rent'], row['rent_amount_paid'], row['water_amount_paid'],
            row['garbage_amount_paid'], row['expenses_on_tenant'], row['total_amount_paid']
            )
                )
                conn.commit()
            except psycopg2.DatabaseError as e:
                my_logger.error(f"Error while inserting row {index}: {str(e)}")

        
    load_rent = PythonOperator(
    task_id="load_rent",
    python_callable=load_rent_data,
    op_args=[True],  # Set to False if you want to ignore duplicates
    dag=dag,
    )
    
    def load_expense_data(handle_duplicates=True):
        df = extract_excel_data(expenses_excel_path)
        hook = PostgresHook(postgres_conn_id = postgres_conn_id)
        conn = hook.get_conn()
        cur = conn.cursor()
        for index,row in df.iterrows():
            try:
                cur.execute("""
                INSERT INTO expenses (
                            table_id,
                            date,
                            expense_category, 
                            expense_label,
                            Amount
                )
                VALUES (%s,%s,%s,%s,%s)
                ON CONFLICT (table_id) DO UPDATE 
                SET
                    table_id = EXCLUDED.table_id,
                    date = EXCLUDED.date,
                    expense_category = EXCLUDED.expense_category,
                    expense_label = EXCLUDED.expense_label,
                    Amount = EXCLUDED.Amount;
                """,
            (
            row['table_id'], row['date'], row['expense_category'], row['expense_label'], row['Amount']
            )
                )
                conn.commit()
            except psycopg2.DatabaseError as e:
                my_logger.error(f"Error while inserting row {index}: {str(e)}")
        
    load_expenses = PythonOperator(
    task_id="load_expenses",
    python_callable=load_expense_data,
    op_args=[True],  # Set to False if you want to ignore duplicates
    dag=dag,
    )
    
    # Define task dependencies
    extract_rent_data >> extract_expenses_data >> get_rent_data >> get_expenses_data >> load_rent >> load_expenses