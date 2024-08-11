import pendulum
import logging
import os

from airflow.decorators import dag, task
from airflow.secrets.metastore import MetastoreBackend
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from final_project_operators.stage_redshift import StageToRedshiftOperator
from final_project_operators.load_fact import LoadFactOperator
from final_project_operators.load_dimension import LoadDimensionOperator
from final_project_operators.data_quality import DataQualityOperator
from udacity.common import final_project_sql_statements



@dag(
    start_date=pendulum.now()
)
def final_project():
    
    stage_events_to_redshift = StageToRedshiftOperator(
        task_id='StageRedshiftEvents',
        redshift_conn_id = "redshift",
        aws_credentials_id = "aws_credentials",
        table = "staging_events",
        json_path = 'JSON \'s3://udacitycoursero2/json/log_json_path.json\'',
        s3_path = "s3://udacitycoursero2/log-data",
        create_staging_table_sql = final_project_sql_statements.SqlQueries.staging_events_table_create
        
    )

    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id='StageSongs',
        redshift_conn_id = "redshift",
        aws_credentials_id = "aws_credentials",
        table = "staging_songs",
        json_path = 'JSON \'auto\'',
        s3_path = "s3://udacitycoursero2/song-data",
        create_staging_table_sql = final_project_sql_statements.SqlQueries.staging_songs_table_create
    )

    load_songplays_table = LoadFactOperator(
        task_id='Load_songplays_fact_table',
        redshift_conn_id='redshift',
        insert_sql= final_project_sql_statements.SqlQueries.songplay_table_insert,
        create_sql=final_project_sql_statements.SqlQueries.songplay_table_create,
        table="songplays"
    )

    load_user_dimension_table = LoadDimensionOperator(
        task_id='Load_user_dim_table',
        redshift_conn_id='redshift',
        create_sql=final_project_sql_statements.SqlQueries.user_table_create,
        insert_sql=final_project_sql_statements.SqlQueries.user_table_insert,
        table="users"
    )

    load_song_dimension_table = LoadDimensionOperator(
        task_id='Load_song_dim_table',
        redshift_conn_id='redshift',
        create_sql=final_project_sql_statements.SqlQueries.song_table_create,
        insert_sql=final_project_sql_statements.SqlQueries.song_table_insert,
        table="song"
    )

    load_artist_dimension_table = LoadDimensionOperator(
        task_id='Load_artist_dim_table',
        redshift_conn_id='redshift',
        create_sql=final_project_sql_statements.SqlQueries.artist_table_create,
        insert_sql=final_project_sql_statements.SqlQueries.artist_table_insert,
        table="artist"
    )

    load_time_dimension_table = LoadDimensionOperator(
        task_id='Load_time_dim_table',
        redshift_conn_id='redshift',
        create_sql=final_project_sql_statements.SqlQueries.time_table_create,
        insert_sql=final_project_sql_statements.SqlQueries.time_table_insert,
        table="time"
    )

    run_quality_checks = DataQualityOperator(
        task_id='Run_data_quality_checks',
        redshift_conn_id='redshift',
        table="users"
    )

    stage_events_to_redshift >> load_songplays_table   
    stage_songs_to_redshift >> load_songplays_table 
    load_songplays_table >> load_user_dimension_table 
    load_songplays_table >> load_song_dimension_table
    load_songplays_table >> load_artist_dimension_table
    load_songplays_table >> load_time_dimension_table
    [load_user_dimension_table, load_song_dimension_table, load_artist_dimension_table, load_time_dimension_table] >> run_quality_checks
final_project_dag = final_project()
