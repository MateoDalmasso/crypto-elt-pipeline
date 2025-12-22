import requests
import pandas as pd
from datetime import datetime
from google.cloud import bigquery
import os

# 1. Configura la conexión
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "google_keys.json"

PROJECT_ID = "crypto-pipeline-2025" 
DATASET_ID = "crypto_raw"
TABLE_ID = "top_cryptos_raw"

def fetch_crypto_data():
    url = "https://api.coingecko.com/api/v3/coins/markets"
    params = {"vs_currency": "usd", "order": "market_cap_desc", "per_page": 10, "page": 1}
    response = requests.get(url, params=params)
    if response.status_code == 200:
        df = pd.DataFrame(response.json())
        cols = ['id', 'symbol', 'name', 'current_price', 'market_cap', 'total_volume', 'last_updated']
        df = df[cols]
        df['extracted_at'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        return df
    return None

def upload_to_bigquery(df):
    client = bigquery.Client()
    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
    
    print(f"Enviando {len(df)} filas a BigQuery...")
    
    # Esta configuración añade los datos a la tabla existente
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")

    job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    job.result() # Espera a que termine
    print("¡DATOS CARGADOS EN BIGQUERY EXITOSAMENTE!")

if __name__ == "__main__":
    df_crypto = fetch_crypto_data()
    if df_crypto is not None:
        print(df_crypto.head()) # Esto es lo que ya ves en consola
        upload_to_bigquery(df_crypto) # <--- ESTO ES LO QUE FALTA EJECUTAR