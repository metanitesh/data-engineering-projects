from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import ( StageToRedshiftOperator, LoadFactOperator, LoadDimensionOperator, DataQualityOperator, PythonOperator, PostgresOperator)


from helpers import SqlQueries
import logging

def start_execution():
    logging.info('starting the execution pipeline')

def end_execution():
    logging.info('starting the execution pipeline')

default_args = {
    'owner': 'udacity',
    'start_date': datetime.now(),
    'schedule_interval':'@hourly',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG('sprakify-data-pipeline',
          default_args=default_args,
          catchup=False,
          description='Load and transform data in Redshift with Airflow',
        )


start_operator = PythonOperator(
    task_id='Begin_execution',  
    dag=dag,
    python_callable=start_execution,
)

create_tables = PostgresOperator(
    task_id="create_redshift_table",
    dag=dag,
    postgres_conn_id="redshift",
    sql=SqlQueries.create_tables
)


stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    table="staging_events",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="udacity-dend",
    s3_key="log_data",
    json_path= 's3://udacity-dend/log_json_path.json'
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    table="staging_songs",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="udacity-dend",
    s3_key="song_data"
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id="redshift",
    query = 'songplay_table_insert'    
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    query='user_table_insert',
    truncate = True,
    table = 'users'
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    query='song_table_insert',
    truncate = True,
    table = 'songs'

)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    query='artist_table_insert',
    truncate = True,
    table = 'artists'
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    query='time_table_insert',
    truncate = True,
    table = 'time'
)


run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    table='users',
    column = 'user_id',
    sql="SELECT COUNT({}) FROM {} WHERE {} IS NULL",
    expected_result = 0
)



end_operator = PythonOperator(
    task_id='End_execution',  
    dag=dag,
    python_callable=end_execution
)


start_operator >>  create_tables
create_tables >> stage_events_to_redshift
create_tables >> stage_songs_to_redshift
stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table
load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table
load_user_dimension_table >> run_quality_checks
load_song_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks
run_quality_checks >> end_operator
