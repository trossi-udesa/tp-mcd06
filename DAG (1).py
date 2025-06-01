from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

### Importar funciones desde el modulo airflow_tasks_pipeline
from airflow_tasks_pipeline import (
    filtrar_datos_fn,
    top_product_fn,
    top_ctr_fn,
    db_writing_fn
)

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 5, 31),
    'retries': 1
}

dag = DAG(
    dag_id='recomendaciones_diarias_pipeline',
    default_args=default_args,
    schedule_interval='0 18 * * *',  # Todos los días a las 21hs Argentina
    catchup=False,
    tags=['recomendaciones', 'pipeline']
)

filtrar_datos = PythonOperator(
    task_id='filtrar_datos',
    python_callable=filtrar_datos_fn,
    dag=dag
)

top_product = PythonOperator(
    task_id='top_product',
    python_callable=top_product_fn,
    dag=dag
)

top_ctr = PythonOperator(
    task_id='top_ctr',
    python_callable=top_ctr_fn,
    dag=dag
)

write_db = PythonOperator(
    task_id='write_to_db',
    python_callable=db_writing_fn,
    dag=dag
)

### Defino el orden de ejecución de las tareas
filtrar_datos >> [top_product, top_ctr] >> write_db
