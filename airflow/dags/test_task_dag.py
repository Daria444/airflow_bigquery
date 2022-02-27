import pandas as pd
import psycopg2
from google_drive_downloader import GoogleDriveDownloader as gdd  
from google.cloud import bigquery
import os 
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
                'owner':'airflow',
                'depends_on_past':False,
                'retries':2,
                'retry_delay':timedelta(minutes=1),
                'start_date':datetime(2022, 2, 24),
                'schedule_interval':'*/2 * * * * '
                }

def get_data():
    gdd.download_file_from_google_drive(file_id='1s0lytpc4ngyCDzj9Gk_bxKQACiWEDIPf', dest_path='./data/transactions.csv')
    transactions = pd.read_csv('./data/transactions.csv')
    gdd.download_file_from_google_drive(file_id='1AAEm_mEbYHDRfV_waYEbuHcA7TU6sgVz', dest_path='./data/users.csv')
    users = pd.read_csv('./data/users.csv')
    gdd.download_file_from_google_drive(file_id='15kHy50x3-0utTvTSCAftKAtMPk9xb9ta', dest_path='./data/webinar.csv')
    webinar = pd.read_csv('./data/webinar.csv')

def upload_csv():
    conn = psycopg2.connect("host=airflow_postgres_1 port=5432 dbname=airflow user=airflow password=airflow")
    cur = conn.cursor()

    q1 = ''' 
            CREATE TABLE IF NOT EXISTS transactions (user_id VARCHAR(10), price int);
            '''
    cur.execute(q1)
    conn.commit()

    with open('./data/transactions.csv', 'r') as f:
        next(f)
        cur.copy_from(f, 'transactions', sep=',', columns=('user_id', 'price'))
    conn.commit()

    q2 = ''' 
            CREATE TABLE IF NOT EXISTS webinar (email VARCHAR(30));
            '''
    cur.execute(q2)
    conn.commit()
    with open('./data/webinar.csv', 'r') as f:
        next(f)
        cur.copy_from(f, 'webinar')
    conn.commit()

    q3 = ''' 
            CREATE TABLE IF NOT EXISTS users (user_id VARCHAR(10), email VARCHAR(30), date_registration TIMESTAMP);
            '''
    cur.execute(q3)
    conn.commit()
    with open('./data/users.csv', 'r') as f:
        next(f)
        cur.copy_from(f, 'users', sep=',', columns=('user_id', 'email', 'date_registration'))
    conn.commit()
    cur.close()
    conn.close()

def create_mat_view():
    conn = psycopg2.connect("host=airflow_postgres_1 port=5432 dbname=airflow user=airflow password=airflow")
    cur = conn.cursor()

    q4 = ''' 
        CREATE MATERIALIZED VIEW  IF NOT EXISTS mat_view AS 
        SELECT
	        users.email as email, 
	        SUM(price) as refill
        FROM users 
        JOIN webinar
        ON users.email = webinar.email
        JOIN transactions
        ON users.user_id = transactions.user_id
        GROUP BY users.email
        HAVING MIN(date_registration) > '2016-04-01';
        '''
    cur.execute(q4)
    conn.commit()   

def send_tab():
    conn = psycopg2.connect("host=airflow_postgres_1 port=5432 dbname=airflow user=airflow password=airflow")
    cur = conn.cursor()
    cur.execute('SELECT * FROM mat_view')
    reg_guests = cur.fetchall()
    cur.close()
    conn.close()

    rows_to_insert = [{'email': i[0], 'refill' : i[1]} for i in reg_guests]

    gdd.download_file_from_google_drive(file_id='1rQkObD7UzZ7bjd49mfJNTIYeGCBHTQmV', dest_path='./data/json_file.json')
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "./data/json_file.json"
    client = bigquery.Client()

    schema = [bigquery.SchemaField("email", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("refill", "INTEGER", mode="REQUIRED")
            ]
    dataset = client.dataset('dataset1')
    table_ref = dataset.table('reg_guests')
    table = bigquery.Table(table_ref, schema=schema)
    table = client.create_table(table)
    table_id = 'my-project-1-342210.dataset1.reg_guests'
    client.insert_rows_json(table_id, rows_to_insert)


dag = DAG('func', default_args=default_args)
t1 = PythonOperator(task_id='get_data', python_callable=get_data, dag=dag)
t2 = PythonOperator(task_id='upload_csv', python_callable=upload_csv, dag=dag)
t3 = PythonOperator(task_id='create_mat_view', python_callable=create_mat_view, dag=dag)
t4 = PythonOperator(task_id='send_tab', python_callable=send_tab, dag=dag)

t1 >> t2 >> t3 >> t4