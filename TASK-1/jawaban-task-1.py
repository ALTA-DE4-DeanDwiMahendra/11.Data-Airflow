##1.Create DAG that run in every 5 hours.

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

def print_hello():
    return 'Hello world from first Airflow DAG!'

# Buat objek DAG
dag = DAG(
    'dag_run_every_5_hours',
    description='DAG to run every 5 hours',
    schedule_interval='0 */5 * * *',  # Menjalankan setiap 5 jam sekali
    start_date=datetime(2024, 8, 4),
    catchup=False
)

# Definisikan operator PythonOperator
operator_hello_world = PythonOperator(
    task_id='hello_task',
    python_callable=print_hello,
    dag=dag
)

# Spesifikasikan urutan task
operator_hello_world

#2. Suppose we define a new task that push a variable to xcom.
def push_var_to_xcom(ti=None):
    ti.xcom_push(key='my_variable_key', value='This is my variable value')

push_var_to_xcom_task = PythonOperator(
    task_id='push_var_to_xcom',
    python_callable=push_var_to_xcom,
    dag=dag
)

#3. How to pull multiple values at once?
def pull_multiple_values_from_xcom(ti=None):
    # Menarik nilai dari xcom berdasarkan key
    values = ti.xcom_pull(task_ids=['task_id_1', 'task_id_2'], key='my_variable_key')

pull_multiple_values_task = PythonOperator(
    task_id='pull_multiple_values_from_xcom',
    python_callable=pull_multiple_values_from_xcom,
    dag=dag
)

