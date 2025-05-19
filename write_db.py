import pandas as pd
from datetime import datetime, timedelta
import logging
import psycopg2

def main():
    #Cloud Storage
    base_path = 'gs://adtech-tp-2025/raw/'

    #Leer raw data de buckets
    advertiser_ids_df = pd.read_csv(base_path + 'advertiser_ids.csv')
    product_views_df = pd.read_csv(base_path + 'product_views.csv')
    ads_views_df = pd.read_csv(base_path + 'ads_views.csv')

    # Preprocesar fechas
    product_views_df['date'] = pd.to_datetime(product_views_df['date'])
    ads_views_df['date'] = pd.to_datetime(ads_views_df['date'])
    impressions_df = ads_views_df[ads_views_df['type'] == 'impression']
    clicks_df = ads_views_df[ads_views_df['type'] == 'click']

    #Conexión a PostgreSQL
    conn = psycopg2.connect(
        host="35.198.4.250",
        dbname="adtech-db-1",
        user="postgres",
        password="(N|Sb?_G#y0]qG8m",
        port=5432
    )
    cursor = conn.cursor()

    # Recorrer los 33 días desde 2025-04-28 hasta 2025-05-31
    start_date = datetime(2025, 4, 28)
    end_date = datetime(2025, 5, 31)
    current_date = start_date

    while current_date <= end_date:
        hoy = pd.to_datetime(current_date.date())
        print(f"Generando recomendaciones para: {hoy.strftime('%Y-%m-%d')}")

        reco1_list = []
        for adv in advertiser_ids_df['advertiser_id'].unique():
            window = product_views_df[
                (product_views_df['advertiser_id'] == adv) &
                (product_views_df['date'] >= hoy - timedelta(days=7)) &
                (product_views_df['date'] <= hoy - timedelta(days=1))
            ]
            if not window.empty:
                top_product = window['product_id'].value_counts().idxmax()
                reco1_list.append({
                    'advertiser_id': adv,
                    'date': hoy.strftime('%Y-%m-%d'),
                    'product_id': top_product,
                })

        reco2_list = []
        for adv in advertiser_ids_df['advertiser_id'].unique():
            imp_window = impressions_df[
                (impressions_df['advertiser_id'] == adv) &
                (impressions_df['date'] >= hoy - timedelta(days=7)) &
                (impressions_df['date'] <= hoy - timedelta(days=1))
            ]
            click_window = clicks_df[
                (clicks_df['advertiser_id'] == adv) &
                (clicks_df['date'] >= hoy - timedelta(days=7)) &
                (clicks_df['date'] <= hoy - timedelta(days=1))
            ]

            if not imp_window.empty:
                imp_count = imp_window.groupby('product_id').size()
                click_count = click_window.groupby('product_id').size()
                ctr_df = pd.DataFrame({'impressions': imp_count, 'clicks': click_count}).fillna(0)
                ctr_df['click_to_impression_ratio'] = ctr_df['clicks'] / ctr_df['impressions']
                top_product = ctr_df['click_to_impression_ratio'].idxmax()
                reco2_list.append({
                    'advertiser_id': adv,
                    'date': hoy.strftime('%Y-%m-%d'),
                    'product_id': top_product
                })

        reco1_df = pd.DataFrame(reco1_list)
        reco2_df = pd.DataFrame(reco2_list)

        # Insertar resultados de reco1_df en la base de datos
        for _, row in reco1_df.iterrows():
            cursor.execute("""
                INSERT INTO top_product (advertiser_id, date, product_reco)
                VALUES (%s, %s, %s)
                ON CONFLICT (advertiser_id, date) DO UPDATE
                SET product_reco = EXCLUDED.product_reco
            """, (row['advertiser_id'], row['date'], row['product_id']))

        print(f"Guardado en DB para {hoy.strftime('%Y-%m-%d')} con {len(reco1_df)} filas.")

        # Insertar resultados de reco2_df en la base de datos
        for _, row in reco2_df.iterrows():
            cursor.execute("""
                INSERT INTO top_ctr (advertiser_id, date, product_reco)
                VALUES (%s, %s, %s)
                ON CONFLICT (advertiser_id, date) DO UPDATE
                SET product_reco = EXCLUDED.product_reco
            """, (row['advertiser_id'], row['date'], row['product_id']))

        print(f"Guardado en DB para {hoy.strftime('%Y-%m-%d')} con {len(reco2_df)} filas.")

        current_date += timedelta(days=1)

    conn.commit()
    cursor.close()
    conn.close()

    logging.info("Todas las recomendaciones generadas y guardadas.")

if __name__ == "__main__":
    main()

print('Todo OK')
