from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.dates import days_ago
import pandas as pd
import numpy as np

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'etl_pipeline',
    default_args=default_args,
    description='An ETL pipeline with data validation and reporting',
    schedule_interval='@daily',
)

# Extract function
def extract():
    # Simulate data extraction from a source
    data = pd.DataFrame({
        'id': range(1, 6),
        'value': np.random.rand(5) * 100
    })
    data.to_csv('/tmp/extracted_data.csv', index=False)

# Transform function
def transform():
    # Read the extracted data
    data = pd.read_csv('/tmp/extracted_data.csv')
    
    # Simple transformation: add a new column
    data['transformed_value'] = data['value'] * 1.1
    
    # Save transformed data
    data.to_csv('/tmp/transformed_data.csv', index=False)

# Load function
def load():
    # Read the transformed data
    data = pd.read_csv('/tmp/transformed_data.csv')
    
    # Simulate loading data into a database or other destination
    print("Loading data...")
    print(data)

# Validate function
def validate():
    # Validate data (e.g., check for missing values)
    data = pd.read_csv('/tmp/transformed_data.csv')
    if data.isnull().sum().sum() > 0:
        raise ValueError("Data validation failed: Missing values found")
    print("Data validation passed")

# Reporting function
def report():
    # Simple reporting
    data = pd.read_csv('/tmp/transformed_data.csv')
    summary = data.describe()
    summary.to_csv('/tmp/report.csv')
    print("Report generated at /tmp/report.csv")

# Define tasks
start = DummyOperator(task_id='start', dag=dag)
extract_task = PythonOperator(task_id='extract', python_callable=extract, dag=dag)
transform_task = PythonOperator(task_id='transform', python_callable=transform, dag=dag)
load_task = PythonOperator(task_id='load', python_callable=load, dag=dag)
validate_task = PythonOperator(task_id='validate', python_callable=validate, dag=dag)
report_task = PythonOperator(task_id='report', python_callable=report, dag=dag)
end = DummyOperator(task_id='end', dag=dag)

# Set task dependencies
start >> extract_task >> transform_task >> validate_task >> load_task >> report_task >> end
