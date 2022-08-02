from asyncore import read
from datetime import datetime, timedelta
import pandas as pd
import json
import pymongo
from pymongo import MongoClient
import gridfs


from airflow import DAG
from airflow.operators.python import PythonOperator

def clean_data():
    tik_tok_reviews = pd.read_csv('data/tiktok_google_play_reviews.csv')
    dropped = tik_tok_reviews.drop(tik_tok_reviews.columns[[8, 9]], axis=1)
    sorted_v = dropped.sort_values(by=['at'], ascending=True)
    replaced = sorted_v.fillna('-').replace(0,"-")
    cleaned = replaced.astype(str).apply(lambda x: x.str.encode('ascii', 'ignore').str.decode('ascii'))
    cleaned.to_json('data/processed_tiktok_google_play_reviews.json', orient='index', indent=2)
    print(cleaned)

def get_processed_file_path(**kwargs):
    kwargs['ti'].xcom_push(key='path', value='data/processed_tiktok_google_play_reviews.json')

def push_to_mongo(**kwargs):
    ti = kwargs['ti']
    path = ti.xcom_pull(key='path', task_ids='get_path')
    print(path)
    db = MongoClient().tiktok
    fs = gridfs.GridFS(db)
    with open(path, 'rb') as file:
        fs.put(file)




default_args = {
    'owner': 'coder2j',
    'retries': 5,
    'retry_delay': timedelta(minutes=2)
    # 'pool': 'task5_pool'
}


with DAG(
    dag_id='our_first_dag_v5',
    default_args=default_args,
    description='This is our first dag that we write',
    start_date=datetime(2022, 8, 1, 2),
    schedule_interval='@daily'
) as dag:
    task1 = PythonOperator(
        task_id='clean_data',
        python_callable=clean_data
    )

    task2 = PythonOperator(
        task_id='push_to_mongo',
        python_callable=push_to_mongo
    )


    task3 = PythonOperator(
        task_id='get_path',
        python_callable=get_processed_file_path
    )

    task1 >> task3 >> task2 


    