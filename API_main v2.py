from fastapi import FastAPI, HTTPException
from typing import List, Dict
import psycopg2
import os
from datetime import datetime, timedelta
from fastapi.responses import JSONResponse

app = FastAPI()

DB_HOST = os.environ.get("DB_HOST", "35.198.4.250")
DB_PORT = os.environ.get("DB_PORT", "5432")
DB_USER = os.environ.get("DB_USER", "postgres")
DB_PASSWORD = os.environ.get("DB_PASSWORD", "(N|Sb?_G#y0]qG8m")
DB_NAME = os.environ.get("DB_NAME", "adtech-db-1")

def get_db_connection():
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            user=DB_USER,
            password=DB_PASSWORD,
            dbname=DB_NAME
        )
        return conn
    except Exception as e:
        print(f"Error connecting to the database: {e}")
        raise HTTPException(status_code=500, detail="Database connection error")

@app.get("/")
def root():
    return {
        "Home": "Recomendaciones AdTech",
        "/recommendations/<ADV>/<Modelo>": "Devuelve la recomendación para el dia actual por advertiser, según el modelo elegido. <Modelo> puede ser top_product o top_ctr. Los cálculos se realizan en b>        "/stats/": "Devuelve estadísticas.",
        "/history/<ADV>/": "Devuelve las recomendaciones por ambos modelos para el advertiser especificado, de los ultimos 7 dias."
    }

@app.get("/recommendations/{adv}/{model}")
def get_recommendations(adv: str, model: str):
    if model not in ["top_product", "top_ctr"]:
        raise HTTPException(status_code=400, detail="Modelo inválido. Use 'top_product' o 'top_ctr'.")

    today = datetime.now().strftime("%Y-%m-%d")

    try:
        conn = get_db_connection()
        cur = conn.cursor()

        query = f"""
            SELECT product_reco, rank
            FROM {model}
            WHERE advertiser_id = %s AND date = %s
            ORDER BY rank ASC
            LIMIT 20
        """
        cur.execute(query, (adv, today))
        rows = cur.fetchall()

        cur.close()
        conn.close()

        if not rows:
            raise HTTPException(status_code=404, detail=f"No recommendations found for advertiser {adv}.")

        recommendations = [{"product_reco": row[0], "rank": row[1]} for row in rows]
        return {"advertiser_id": adv, "date": today, "recommendations": recommendations}

    except Exception as e:
        print(f"Error fetching recommendations: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.get("/stats/")
def get_stats():
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        today = datetime.now().strftime("%Y-%m-%d")

        cur.execute("SELECT COUNT(DISTINCT advertiser_id) FROM top_product WHERE date <= %s", (today,))
        total_advertisers = cur.fetchone()[0]

        cur.execute(
            """
            SELECT advertiser_id, COUNT(DISTINCT product_reco) AS variation
            FROM top_product
            WHERE date <= %s
            GROUP BY advertiser_id
            ORDER BY variation DESC
            LIMIT 5
            """,
            (today,)
        )
        most_variable_advertisers = cur.fetchall()

        cur.execute(
            """
            SELECT product_reco, COUNT(*) AS count
            FROM top_product
            WHERE date <= %s
            GROUP BY product_reco
            ORDER BY count DESC
            LIMIT 5
            """,
            (today,)
        )
        top_5_top_product = cur.fetchall()

        cur.execute(
            """
            SELECT product_reco, COUNT(*) AS count
            FROM top_ctr
            WHERE date <= %s
            GROUP BY product_reco
            ORDER BY count DESC
            LIMIT 5
            """,
            (today,)
        )
        top_5_top_ctr = cur.fetchall()

        stats = {
            "total_advertisers_activos": total_advertisers,
            "advertisers_mas_variables": [
                {"advertiser_id": row[0], "unique_recos_top_product": row[1]}
                for row in most_variable_advertisers
            ],
            "top_5_top_product": [
                {"product_reco": row[0], "count": row[1]}
                for row in top_5_top_product
            ],
            "top_5_top_ctr": [
                {"product_reco": row[0], "count": row[1]}
                for row in top_5_top_ctr
            ]
        }

        cur.close()
        conn.close()

        return stats

    except Exception as e:
        print(f"Error fetching stats: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

    except Exception as e:
        print(f"Error fetching stats: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.get("/history/{adv}")
def get_history(adv: str):
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        seven_days_ago = (datetime.now() - timedelta(days=6)).strftime("%Y-%m-%d")
        today = datetime.now().strftime("%Y-%m-%d")

        # Top Product
        query_top_product = """
            SELECT date, product_reco, rank
            FROM top_product
            WHERE advertiser_id = %s
            AND date >= %s
            AND date <= %s
            ORDER BY date DESC, rank ASC
            LIMIT 140  -- 7 days * 20 products per day
        """
        cur.execute(query_top_product, (adv, seven_days_ago, today))
        top_product_rows = cur.fetchall()

        # Top CTR
        query_top_ctr = """
            SELECT date, product_reco, rank
            FROM top_ctr
            WHERE advertiser_id = %s
            AND date >= %s
            AND date <= %s
            ORDER BY date DESC, rank ASC
            LIMIT 140  -- 7 days * 20 products per day
        """
        cur.execute(query_top_ctr, (adv, seven_days_ago, today))
        top_ctr_rows = cur.fetchall()

        cur.close()
        conn.close()

        if not top_product_rows and not top_ctr_rows:
            raise HTTPException(status_code=404, detail=f"No history found for advertiser {adv}.")

        top_product_history = [
            {
                "date": row[0].strftime("%Y-%m-%d"),
                "product_reco": row[1],
                "rank": row[2]
            }
            for row in top_product_rows
        ]

        top_ctr_history = [
            {
                "date": row[0].strftime("%Y-%m-%d"),
                "product_reco": row[1],
                "rank": row[2]
            }
            for row in top_ctr_rows
        ]

        return {
            "advertiser_id": adv,
            "top_product_history": top_product_history,
            "top_ctr_history": top_ctr_history
        }

    except Exception as e:
        print(f"Error fetching history: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

