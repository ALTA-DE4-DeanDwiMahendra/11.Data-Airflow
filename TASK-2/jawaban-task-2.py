#Jawaban untuk Task-2
from airflow import DAG
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime
import json

dag = DAG('gender_prediction_dag', description='DAG untuk memprediksi gender menggunakan Gender API',
          schedule_interval='@once',
          start_date=datetime(2024, 1, 1), catchup=False)

predict_names = SimpleHttpOperator(
    task_id='predict_names',
    endpoint='/gender/by-first-name-multiple',
    method='POST',
    data='{"country": "ID", "locale": null, "ip": null, "first_name": "Musa"}',
    http_conn_id='gender_api',
    log_response=True,
    dag=dag
)

create_table_task = PostgresOperator(
    task_id='create_gender_prediction_table',
    sql='''
        CREATE TABLE IF NOT EXISTS gender_name_prediction (
            input JSON,
            details JSON,
            result_found BOOLEAN,
            first_name VARCHAR,
            probability FLOAT,
            gender VARCHAR,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    ''',
    postgres_conn_id='pg_conn_id',
    autocommit=True,
    dag=dag
)

def load_predictions_to_postgres():
    pg_hook = PostgresHook(postgres_conn_id='pg_conn_id').get_conn()
    curr = pg_hook.cursor("cursor")
    predictions = [
        {
            "input": {"first_name": "sandra", "country": "US"},
            "details": {
                "credits_used": 1,
                "duration": "13ms",
                "samples": 9273,
                "country": "US",
                "first_name_sanitized": "sandra"
            },
            "result_found": True,
            "first_name": "Sandra",
            "probability": 0.98,
            "gender": "female"
        }
    ]
    for prediction in predictions:
        curr.execute('''
            INSERT INTO gender_name_prediction (input, details, result_found, first_name, probability, gender)
            VALUES (%s, %s, %s, %s, %s, %s)
        ''', (json.dumps(prediction['input']), json.dumps(prediction['details']), prediction['result_found'],
              prediction['first_name'], prediction['probability'], prediction['gender']))
    pg_hook.commit()

load_predictions_task = PythonOperator(
    task_id='load_predictions_to_postgres',
    python_callable=load_predictions_to_postgres,
    dag=dag
)

predict_names >> create_table_task >> load_predictions_task
