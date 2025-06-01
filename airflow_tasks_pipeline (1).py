import pandas as pd
import gcsfs
import psycopg2
from datetime import datetime, timedelta

def filtrar_datos_fn(**context):
    service_account_info = {key} ## No agregada por motivos de seguridad
    fs = gcsfs.GCSFileSystem(token=service_account_info)

    base_path = 'gs://adtech-tp-2025/raw/'
    output_path = 'gs://adtech-tp-2025/filtered_df/'

    advertiser_ids_df = pd.read_csv(fs.open(base_path + 'advertiser_ids.csv'))
    product_views_df = pd.read_csv(fs.open(base_path + 'product_views.csv'))
    ads_views_df = pd.read_csv(fs.open(base_path + 'ads_views.csv'))

    active_advertisers = set(advertiser_ids_df['advertiser_id'])
    product_views_filtered = product_views_df[product_views_df['advertiser_id'].isin(active_advertisers)]
    ads_views_filtered = ads_views_df[ads_views_df['advertiser_id'].isin(active_advertisers)]

    with fs.open(output_path + 'product_views_filtered.csv', 'w') as f:
        product_views_filtered.to_csv(f, index=False)

    with fs.open(output_path + 'ads_views_filtered.csv', 'w') as f:
        ads_views_filtered.to_csv(f, index=False)

    print("Filtrado completado y archivos guardados en GCS.")

def top_product_fn(**context):
    service_account_info = {key} ## No agregada por motivos de seguridad
    fs = gcsfs.GCSFileSystem(token=service_account_info)

    hoy = datetime(2025, 5, 31)
    start_window = hoy - timedelta(days=7)
    end_window = hoy - timedelta(days=1)

    df = pd.read_csv(fs.open('gs://adtech-tp-2025/filtered_df/product_views_filtered.csv'))
    df['date'] = pd.to_datetime(df['date'])
    df = df[(df['date'] >= start_window) & (df['date'] <= end_window)]

    grouped = df.groupby(['advertiser_id', 'product_id']).size().reset_index(name='views')
    grouped['rank'] = grouped.groupby('advertiser_id')['views'].rank(method='first', ascending=False)
    top20 = grouped[grouped['rank'] <= 20].copy()
    top20['date'] = hoy.strftime('%Y-%m-%d')

    output_path = f'gs://adtech-tp-2025/top/top_product/top_product_{hoy.strftime("%Y-%m-%d")}.csv'
    with fs.open(output_path, 'w') as f:
        top20[['advertiser_id', 'date', 'product_id', 'rank']].to_csv(f, index=False)

    print("TopProduct generado y guardado en GCS")

def top_ctr_fn(**context):
    service_account_info = {key} ## No agregada por motivos de seguridad
    fs = gcsfs.GCSFileSystem(token=service_account_info)

    hoy = datetime(2025, 5, 31)
    start_window = hoy - timedelta(days=7)
    end_window = hoy - timedelta(days=1)

    df = pd.read_csv(fs.open('gs://adtech-tp-2025/filtered_df/ads_views_filtered.csv'))
    df['date'] = pd.to_datetime(df['date'])
    df = df[(df['date'] >= start_window) & (df['date'] <= end_window)]

    impressions = df[df['type'] == 'impression']
    clicks = df[df['type'] == 'click']

    impressions_count = impressions.groupby(['advertiser_id', 'product_id']).size().reset_index(name='impressions')
    clicks_count = clicks.groupby(['advertiser_id', 'product_id']).size().reset_index(name='clicks')

    ctr_df = pd.merge(impressions_count, clicks_count, on=['advertiser_id', 'product_id'], how='left').fillna(0)
    ctr_df['ctr'] = ctr_df['clicks'] / ctr_df['impressions']
    ctr_df['rank'] = ctr_df.groupby('advertiser_id')['ctr'].rank(method='first', ascending=False)

    top20 = ctr_df[ctr_df['rank'] <= 20].copy()
    top20['date'] = hoy.strftime('%Y-%m-%d')

    output_path = f'gs://adtech-tp-2025/top/top_ctr/top_ctr_{hoy.strftime("%Y-%m-%d")}.csv'
    with fs.open(output_path, 'w') as f:
        top20[['advertiser_id', 'date', 'product_id', 'rank']].to_csv(f, index=False)

    print("TopCTR generado y guardado en GCS")

def db_writing_fn(**context):
    hoy = datetime(2025, 5, 31)
    fecha_str = hoy.strftime('%Y-%m-%d')

    top_product_path = f'gs://adtech-tp-2025/top/top_product/top_product_{fecha_str}.csv'
    top_ctr_path = f'gs://adtech-tp-2025/top/top_ctr/top_ctr_{fecha_str}.csv'

    top_product_df = pd.read_csv(top_product_path)
    top_ctr_df = pd.read_csv(top_ctr_path)

    conn = psycopg2.connect(   ### Datos no agregados por motivos de seguridad
        host="IP",
        dbname="Nombre DB",
        user="postgres",
        password="Contraseña",
        port=5432
    )
    cur = conn.cursor()

    for _, row in top_product_df.iterrows():
        cur.execute("""
            INSERT INTO top_product (advertiser_id, date, product_reco, rank)
            VALUES (%s, %s, %s, %s)
        """, (row['advertiser_id'], row['date'], row['product_id'], row['rank']))

    for _, row in top_ctr_df.iterrows():
        cur.execute("""
            INSERT INTO top_ctr (advertiser_id, date, product_reco, rank)
            VALUES (%s, %s, %s, %s)
        """, (row['advertiser_id'], row['date'], row['product_id'], row['rank']))

    conn.commit()
    cur.close()
    conn.close()

    print("Datos insertados en Cloud SQL correctamente")
