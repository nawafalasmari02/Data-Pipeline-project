from datetime import timedelta
import pendulum

from airflow.decorators import dag
from airflow.operators.dummy_operator import DummyOperator
from airflow.models import Variable

from final_project_operators.stage_redshift import StageToRedshiftOperator
from final_project_operators.load_fact import LoadFactOperator
from final_project_operators.load_dimension import LoadDimensionOperator
from final_project_operators.data_quality import DataQualityOperator

from udacity.common import final_project_sql_statements as sql_statements


# Airflow Variables (set via set_connections_and_variables.sh or UI)
S3_BUCKET = Variable.get("s3_bucket")
S3_PREFIX = Variable.get("s3_prefix", default_var="")

# Airflow Connection IDs
AWS_CONN_ID = "aws_credentials"
REDSHIFT_CONN_ID = "redshift"

AWS_REGION = "us-west-2"


def _with_prefix(path: str) -> str:
    if not S3_PREFIX:
        return path
    return f"{S3_PREFIX.rstrip('/')}/{path.lstrip('/')}"


default_args = {
    "owner": "udacity",
    "depends_on_past": False,
    "start_date": pendulum.datetime(2023, 1, 1, tz="UTC"),
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "catchup": False,
    "email_on_retry": False,
}

@dag(
    default_args=default_args,
    description="Load and transform data in Redshift with Airflow",
    schedule_interval="0 * * * *",
)
def final_project():

    start_operator = DummyOperator(task_id="Begin_execution")
    end_operator = DummyOperator(task_id="Stop_execution")

    stage_events_to_redshift = StageToRedshiftOperator(
        task_id="Stage_events",
        redshift_conn_id=REDSHIFT_CONN_ID,
        aws_conn_id=AWS_CONN_ID,
        table="staging_events",
        s3_bucket=S3_BUCKET,
        s3_key=_with_prefix("log-data"),
        json_path=f"s3://{S3_BUCKET}/{_with_prefix('log_json_path.json')}",
        region=AWS_REGION,
        truncate_table=True,
    )

    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id="Stage_songs",
        redshift_conn_id=REDSHIFT_CONN_ID,
        aws_conn_id=AWS_CONN_ID,
        table="staging_songs",
        s3_bucket=S3_BUCKET,
        s3_key=_with_prefix("song-data"),
        json_path="auto",
        region=AWS_REGION,
        truncate_table=True,
    )

    load_songplays_table = LoadFactOperator(
        task_id="Load_songplays_fact_table",
        redshift_conn_id=REDSHIFT_CONN_ID,
        table="songplays",
        sql=sql_statements.SqlQueries.songplay_table_insert,
    )

    load_user_dimension_table = LoadDimensionOperator(
        task_id="Load_user_dim_table",
        redshift_conn_id=REDSHIFT_CONN_ID,
        table="users",
        sql=sql_statements.SqlQueries.user_table_insert,
        truncate_insert=True,
    )

    load_song_dimension_table = LoadDimensionOperator(
        task_id="Load_song_dim_table",
        redshift_conn_id=REDSHIFT_CONN_ID,
        table="songs",
        sql=sql_statements.SqlQueries.song_table_insert,
        truncate_insert=True,
    )

    load_artist_dimension_table = LoadDimensionOperator(
        task_id="Load_artist_dim_table",
        redshift_conn_id=REDSHIFT_CONN_ID,
        table="artists",
        sql=sql_statements.SqlQueries.artist_table_insert,
        truncate_insert=True,
    )

    load_time_dimension_table = LoadDimensionOperator(
        task_id="Load_time_dim_table",
        redshift_conn_id=REDSHIFT_CONN_ID,
        table="time",
        sql=sql_statements.SqlQueries.time_table_insert,
        truncate_insert=True,
    )

    run_quality_checks = DataQualityOperator(
        task_id="Run_data_quality_checks",
        redshift_conn_id=REDSHIFT_CONN_ID,
        tests=[
            {"check_sql": "SELECT COUNT(*) FROM songplays;", "expected_result": 0, "comparison": ">"},
            {"check_sql": "SELECT COUNT(*) FROM users;", "expected_result": 0, "comparison": ">"},
            {"check_sql": "SELECT COUNT(*) FROM users WHERE userid IS NULL;", "expected_result": 0, "comparison": "=="},
        ],
    )

    start_operator >> [stage_events_to_redshift, stage_songs_to_redshift]
    [stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table
    load_songplays_table >> [
        load_user_dimension_table,
        load_song_dimension_table,
        load_artist_dimension_table,
        load_time_dimension_table,
    ]
    [
        load_user_dimension_table,
        load_song_dimension_table,
        load_artist_dimension_table,
        load_time_dimension_table,
    ] >> run_quality_checks
    run_quality_checks >> end_operator


final_project_dag = final_project()
