from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from google.cloud import storage
import pandas as pd
import psycopg2
from datetime import datetime, timedelta
import os

# Configuración de conexión a GCP y SQL
GCP_BUCKET = 'adtech-bucket-1'
GCP_PROJECT = 'tp-mcd06-1'
POSTGRES_HOST = '35.198.4.250'
POSTGRES_PORT = '5432'
POSTGRES_DB = 'adtech'
POSTGRES_USER = 'postgres'
POSTGRES_PASSWORD = '(N|Sb?_G#y0]qG8m'

# Función para descargar archivos desde GCP Storage
def download_files_from_gcs():
    client = storage.Client(project=GCP_PROJECT)
    bucket = client.bucket(GCP_BUCKET)

    blobs = ["advertiser_ids.csv", "product_views.csv", "ads_views.csv"]
    for blob_name in blobs:
        blob = bucket.blob(blob_name)
        blob.download_to_filename(f"/tmp/{blob_name}")
    print("Archivos descargados correctamente")

# Función para calcular recomendaciones
def calcular_recomendaciones():
    # Importar los datos
    advertiser_ids_df = pd.read_csv('/tmp/advertiser_ids.csv')
    product_views_df = pd.read_csv('/tmp/product_views.csv')
    ads_views_df = pd.read_csv('/tmp/ads_views.csv')

    # Filtrar por la última semana
    today = datetime.now() - timedelta(days=1)
    seven_days_ago = today - timedelta(days=6)
    today_str = today.strftime('%Y-%m-%d')
    start_date_str = seven_days_ago.strftime('%Y-%m-%d')

    # Top Product (Reco 1)
    product_views_df['date'] = pd.to_datetime(product_views_df['date'])
    filtered_views = product_views_df[(product_views_df['date'] >= start_date_str) & (product_views_df['date'] <= today_str)]

    reco1_list = []
    for advertiser_id in advertiser_ids_df['advertiser_id'].unique():
        top_product = filtered_views[filtered_views['advertiser_id'] == advertiser_id]['product_id'].value_counts().idxmax()
        reco1_list.append({
            'advertiser_id': advertiser_id,
            'date': today_str,
            'product_reco': top_product
        })

    reco1_df = pd.DataFrame(reco1_list)

    # Top CTR (Reco 2)
    ads_views_df['date'] = pd.to_datetime(ads_views_df['date'])
    filtered_ads = ads_views_df[(ads_views_df['date'] >= start_date_str) & (ads_views_df['date'] <= today_str)]

    impressions_df = filtered_ads[filtered_ads['type'] == 'impression']
    clicks_df = filtered_ads[filtered_ads['type'] == 'click']

    reco2_list = []
    for advertiser_id in advertiser_ids_df['advertiser_id'].unique():
        impressions_count = impressions_df[impressions_df['advertiser_id'] == advertiser_id].groupby('product_id').size()
        clicks_count = clicks_df[clicks_df['advertiser_id'] == advertiser_id].groupby('product_id').size()
        ctr_df = pd.DataFrame({'impressions': impressions_count, 'clicks': clicks_count}).fillna(0)
        ctr_df['click_to_impression_ratio'] = ctr_df['clicks'] / ctr_df['impressions']

        if not ctr_df.empty:
            top_product = ctr_df['click_to_impression_ratio'].idxmax()
            reco2_list.append({
                'advertiser_id': advertiser_id,
                'date': today_str,
                'product_reco': top_product
            })

    reco2_df = pd.DataFrame(reco2_list)

    # Guardar en la base de datos
    conn = psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD
    )
    cursor = conn.cursor()

    # Insertar los datos de reco1_df (sin borrar los existentes)
    for _, row in reco1_df.iterrows():
        cursor.execute(
            """
            INSERT INTO top_product (advertiser_id, date, product_reco)
            VALUES (%s, %s, %s)
            ON CONFLICT (advertiser_id, date)
            DO UPDATE SET product_reco = EXCLUDED.product_reco;
            """,
            (row['advertiser_id'], row['date'], row['product_reco'])
        )

    # Insertar los datos de reco2_df (sin borrar los existentes)
    for _, row in reco2_df.iterrows():
        cursor.execute(
            """
            INSERT INTO top_ctr (advertiser_id, date, product_reco)
            VALUES (%s, %s, %s)
            ON CONFLICT (advertiser_id, date)
            DO UPDATE SET product_reco = EXCLUDED.product_reco;
            """,
            (row['advertiser_id'], row['date'], row['product_reco'])
        )

    conn.commit()
    cursor.close()
    conn.close()

    print("Datos guardados correctamente en la base de datos")

# Definir el DAG
default_args = {
    'owner': 'trossi',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='adtech_dag',
    default_args=default_args,
    description='DAG para generar recomendaciones',
    schedule_interval='1 0 * * *',  # Todos los días a las 00:01
    start_date=days_ago(1),
    catchup=False,
    tags=['adtech', 'reco']
) as dag:

    download_task = PythonOperator(
        task_id='download_files_from_gcs',
        python_callable=download_files_from_gcs
    )

    reco_task = PythonOperator(
        task_id='calcular_recomendaciones',
        python_callable=calcular_recomendaciones
    )

    download_task >> reco_task
