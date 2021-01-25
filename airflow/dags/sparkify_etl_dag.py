from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2019, 1, 12),
    'depends_on_past': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5)

}

data_quality_check_list=[
        { 'name': 'userid is never null', 'sql_stmt': 'SELECT COUNT(*) FROM public.songplays WHERE userid IS NULL', 'expected_result': 0, 'comparator': 'eq' }, 
        { 'name': 'userid is never null', 'sql_stmt': 'SELECT COUNT(DISTINCT "level") FROM public.songplays', 'expected_result': 2, 'comparator': 'eq' },
        { 'name': 'userid is never null', 'sql_stmt': 'SELECT COUNT(*) FROM public.artists WHERE name IS NULL', 'expected_result': 0, 'comparator': 'eq' },
        { 'name': 'userid is never null', 'sql_stmt': 'SELECT COUNT(*) FROM public.songs WHERE title IS NULL', 'expected_result': 0, 'comparator': 'eq' },
        { 'name': 'userid is never null', 'sql_stmt': 'SELECT COUNT(*) FROM public.users WHERE first_name IS NULL', 'expected_result': 0, 'comparator': 'eq' },
        { 'name': 'userid is never null', 'sql_stmt': 'SELECT COUNT(*) FROM public."time" WHERE weekday IS NULL', 'expected_result': 0, 'comparator': 'eq' },
        { 'name': 'userid is never null', 'sql_stmt': 'SELECT COUNT(*) FROM public.songplays sp LEFT OUTER JOIN public.users u ON u.userid = sp.userid WHERE u.userid IS NULL', \
         'expected_result': 0, 'comparator': 'eq' }
    ],

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *',
          catchup=False
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="staging_events",
    s3_bucket="udacity-dend",
    s3_key="og_data",
    extra_params="FORMAT AS JSON 's3://udacity-dend/log_json_path.json'"
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="staging_songs",
    s3_bucket="udacity-dend",
    s3_key="song_data",
    extra_params="JSON 'auto' COMPUPDATE OFF",
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id="redshift",
    fact_table="songplays",
    reload=True,
    select_sql_stmt=SqlQueries.songplay_table_insert
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    dimension_table="user",
    reload=True,
    select_sql_stmt=SqlQueries.user_table_insert

)

load_song_dimension_table = LoadDimensionOperator(
    task_id="Load_song_dim_table",
    dag=dag,
    redshift_conn_id="redshift",
    dimension_table="song",
    reload=True,
    select_sql_stmt=SqlQueries.song_table_insert
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id="Load_artist_dim_table",
    dag=dag,
    redshift_conn_id="redshift",
    dimension_table="artist",
    reload=True,
    select_sql_stmt=SqlQueries.artist_table_insert
)

load_time_dimension_table = LoadDimensionOperator(
    task_id="Load_time_dim_table",
    dag=dag,
    redshift_conn_id="redshift",
    dimension_table="time",
    reload=True,
    select_sql_stmt=SqlQueries.time_table_insert
)

run_quality_checks = DataQualityOperator(
    task_id="Run_data_quality_checks",
    dag=dag,
    redshift_conn_id="redshift",
    check_list = data_quality_check_list
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

#
# Tasks ordering
#

start_operator >> stage_events_to_redshift
start_operator >> stage_songs_to_redshift

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