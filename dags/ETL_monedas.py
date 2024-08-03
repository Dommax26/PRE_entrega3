from datetime import timedelta, datetime
import json
import requests
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import os

# Configuración de la conexión a Redshift usando variables de entorno
redshift_conn = {
    'host': os.getenv('REDSHIFT_HOST'),
    'username': os.getenv('REDSHIFT_USER'),
    'database': os.getenv('REDSHIFT_DB'),
    'port': '5439',
    'pwd': os.getenv('REDSHIFT_PASSWORD')
}

# Definición del DAG
default_args = {
    'owner': 'DavidBU',
    'start_date': datetime(2023, 5, 30),
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

BC_dag = DAG(
    dag_id='ETL_monedas',
    default_args=default_args,
    description='ETL para tasas de cambio de divisas',
    schedule_interval="@daily",
    catchup=False
)

def fetch_exchange_rates(exec_date):
    try:
        print(f"Adquiriendo data para la fecha: {exec_date}")
        base_url = "https://api.exchangerate-api.com/v4/latest/"
        base_currency = "USD"
        url = base_url + base_currency
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            return data
        else:
            raise Exception(f"Error al obtener datos de la API: {response.status_code}")
    except Exception as e:
        print(f"Error en la extracción de datos: {e}")
        raise e

def process_data(exec_date, data):
    base_currency = "USD"
    target_currencies = ["EUR", "GBP", "JPY", "CAD", "MXN"]
    base_rate = data["rates"].get(base_currency, 1)
    rates = {currency: data["rates"].get(currency) for currency in target_currencies}
    converted_rates = {currency: rate / base_rate for currency, rate in rates.items() if rate is not None}

    today = datetime.date.today()
    df = pd.DataFrame({
        "fecha": [today],
        "moneda_base": [base_currency],
        **converted_rates
    })
    return df

def store_data_in_db(df, exec_date):
    conn = None
    try:
        conn = psycopg2.connect(
            host=redshift_conn['host'],
            dbname=redshift_conn["database"],
            user=redshift_conn["username"],
            password=redshift_conn["pwd"],
            port='5439'
        )
        cursor = conn.cursor()

        cursor.execute("""
        CREATE TABLE IF NOT EXISTS tipos_cambio (
            fecha DATE,
            moneda_base VARCHAR(3),
            EUR DECIMAL(10,4),
            GBP DECIMAL(10,4),
            JPY DECIMAL(10,4),
            CAD DECIMAL(10,4),
            MXN DECIMAL(10,4)
        )
        """)

        for index, row in df.iterrows():
            cursor.execute("""
            INSERT INTO tipos_cambio (fecha, moneda_base, EUR, GBP, JPY, CAD, MXN)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (row["fecha"], row["moneda_base"], row.get("EUR"), row.get("GBP"), row.get("JPY"), row.get("CAD"),
                  row.get("MXN")))

        conn.commit()
    except Exception as e:
        print(f"Error al conectar o insertar en la base de datos: {e}")
        raise e
    finally:
        if conn:
            cursor.close()
            conn.close()

# Funciones de Airflow
def extract_task(exec_date):
    data = fetch_exchange_rates(exec_date)
    df = process_data(exec_date, data)
    df.to_csv('/opt/airflow/processed_data/' + f"data_{datetime.date.today()}.csv", index=False)
    return df

def load_task(exec_date):
    df = pd.read_csv('/opt/airflow/processed_data/' + f"data_{datetime.date.today()}.csv")
    conn = psycopg2.connect(
        host=redshift_conn['host'],
        dbname=redshift_conn["database"],
        user=redshift_conn["username"],
        password=redshift_conn["pwd"],
        port='5439'
    )
    columns = ['fecha', 'moneda_base', 'EUR', 'GBP', 'JPY', 'CAD', 'MXN']
    values = [tuple(x) for x in df.to_numpy()]
    insert_sql = f"INSERT INTO tipos_cambio ({', '.join(columns)}) VALUES %s"
    cur = conn.cursor()
    cur.execute("BEGIN")
    execute_values(cur, insert_sql, values)
    cur.execute("COMMIT")
    cur.close()
    conn.close()

# Definición de tareas
task_1 = PythonOperator(
    task_id='fetch_data',
    python_callable=extract_task,
    op_args=["{{ ds }}"],
    dag=BC_dag,
)

task_2 = PythonOperator(
    task_id='load_data',
    python_callable=load_task,
    op_args=["{{ ds }}"],
    dag=BC_dag,
)

# Definición del orden de las tareas
task_1 >> task_2

