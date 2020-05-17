from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                               LoadDimensionOperator, DataQualityOperator)

from helpers import SqlQueries


ON_FAILURE_RETRIES = 3

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2019, 1, 12),
    'depends_on_past': False,
    'retries': ON_FAILURE_RETRIES,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False,
}

with DAG(
    'udac_example_dag',
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval='0 * * * *',
    catchup=False,
) as dag:
    start_operator = DummyOperator(task_id='Begin_execution', dag=dag)

    stage_events_to_redshift = StageToRedshiftOperator(
        task_id='Stage_events',
        dag=dag,
        table="staging_events",
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        s3_bucket="udacity-dend",
        s3_key="log_data",
        json_path="s3://udacity-dend/log_json_path.json",
        file_type="json"
    )

    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id='Stage_songs',
        dag=dag,
        table="staging_songs",
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        s3_bucket="udacity-dend",
        s3_key="song_data/A/A/A",
        json_path="auto",
        file_type="json"
    )

    load_songplays_table = LoadFactOperator(
        task_id='Load_songplays_fact_table',
        dag=dag,
        table='songplays',
        redshift_conn_id="redshift",
        load_sql=SqlQueries.songplay_table_insert
    )

    load_user_dimension_table = LoadDimensionOperator(
        task_id='Load_user_dim_table',
        dag=dag,
        table='users',
        redshift_conn_id="redshift",
        load_sql=SqlQueries.user_table_insert
    )

    load_song_dimension_table = LoadDimensionOperator(
        task_id='Load_song_dim_table',
        dag=dag,
        table='songs',
        redshift_conn_id="redshift",
        load_sql=SqlQueries.song_table_insert
    )

    load_artist_dimension_table = LoadDimensionOperator(
        task_id='Load_artist_dim_table',
        dag=dag,
        table='artists',
        redshift_conn_id="redshift",
        load_sql=SqlQueries.artist_table_insert
    )

    load_time_dimension_table = LoadDimensionOperator(
        task_id='Load_time_dim_table',
        dag=dag,
        table='time',
        redshift_conn_id="redshift",
        load_sql=SqlQueries.time_table_insert
    )

    run_quality_checks = DataQualityOperator(
        task_id='Run_data_quality_checks',
        dag=dag,
    )

    end_operator = DummyOperator(task_id='Stop_execution', dag=dag)

    # forward pipeline order
    start_operator >> [stage_events_to_redshift, stage_songs_to_redshift]

    # backwards pipeline order
    load_songplays_table << [stage_events_to_redshift, stage_songs_to_redshift]

    # backwards pipeline order
    load_songplays_table >> [
        load_song_dimension_table,
        load_user_dimension_table,
        load_artist_dimension_table,
        load_time_dimension_table
    ]

    # backwards pipeline order
    run_quality_checks << [
        load_song_dimension_table,
        load_user_dimension_table,
        load_artist_dimension_table,
        load_time_dimension_table
    ]

run_quality_checks >> end_operator
