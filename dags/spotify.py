import base64
import json
import os
from datetime import datetime

import pandas as pd
import pytz
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from requests import get, post


def check_and_create_file():
    file_path = '/opt/airflow/data/spotify_Top50_infos.csv'
    if not os.path.exists(file_path):
        with open(file_path, 'w') as file:
            file.write('Nome da música,Artistas,Data do top 50,Popularidade\n')
        print(f"Arquivo {file_path} criado com sucesso.")
    else:
        print(f"O arquivo {file_path} já existe.")


def get_token():
    auth_str = f'{Variable.get("CLIENT_ID")}:{Variable.get("CLIENT_SECRET")}'
    b64_auth_str = base64.urlsafe_b64encode(auth_str.encode()).decode()

    url = 'https://accounts.spotify.com/api/token'
    headers = {
        'Authorization': f'Basic {b64_auth_str}',
        'Content-Type': 'application/x-www-form-urlencoded'
    }
    payload = {
        'grant_type': 'client_credentials'
    }

    r = post(url, headers=headers, data=payload)
    return json.loads(r.content)['access_token']


def get_auth_header(**kwargs):
    token = kwargs['ti'].xcom_pull(task_ids='get_token_task')
    return {'Authorization': f'Bearer {token}'}


def search_id(**kwargs):
    headers = kwargs['ti'].xcom_pull(task_ids='get_auth_header_task')
    name = Variable.get('PLAYLIST_NAME')

    url = 'https://api.spotify.com/v1/search'
    query = f'?q={name}&type=playlist&limit=1'

    r = get(url + query, headers=headers)
    return json.loads(r.content)['playlists']['items'][0]['id']


def search_info(**kwargs):
    headers = kwargs['ti'].xcom_pull(task_ids='get_auth_header_task')
    id = kwargs['ti'].xcom_pull(task_ids='search_id_task')

    url = f'https://api.spotify.com/v1/playlists/{id}'

    r = get(url, headers=headers)
    data = json.loads(r.content)

    tracks_info = []

    time_zone = pytz.timezone('America/Sao_Paulo')
    for track in data['tracks']['items']:
        name_track = track['track']['name']
        artists = [artist['name'] for artist in track['track']['artists']]
        current_date = datetime.now().astimezone(time_zone).strftime('%Y-%m-%d')
        popularity = track['track']['popularity'] if 'popularity' in track['track'] else 0

        tracks_info.append([name_track, ', '.join(artists), current_date, popularity])

    df = pd.DataFrame(tracks_info, columns=['Nome da música', 'Artistas', 'Data do top 50', 'Popularidade'])
    df.to_csv('/opt/airflow/data/spotify_Top50_infos.csv', index=False, mode='a', header=False)


dag = DAG('spotify_dag', description='Spotify DAG',
          schedule_interval='@daily', start_date=datetime(2023, 8, 23),
          catchup=False)

check_and_create_file_task = PythonOperator(
    task_id='check_and_create_file_task',
    python_callable=check_and_create_file,
    dag=dag,
)

get_token_task = PythonOperator(
    task_id='get_token_task',
    python_callable=get_token,
    dag=dag,
)

get_auth_header_task = PythonOperator(
    task_id='get_auth_header_task',
    python_callable=get_auth_header,
    provide_context=True,
    dag=dag,
)

search_id_task = PythonOperator(
    task_id='search_id_task',
    python_callable=search_id,
    provide_context=True,
    dag=dag,
)

search_info_task = PythonOperator(
    task_id='search_info_task',
    python_callable=search_info,
    provide_context=True,
    dag=dag,
)


check_and_create_file_task >> get_token_task
get_token_task >> get_auth_header_task
get_auth_header_task >> search_id_task
search_id_task >> search_info_task
