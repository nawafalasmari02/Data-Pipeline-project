from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.hooks.postgres_hook import PostgresHook
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook


class StageToRedshiftOperator(BaseOperator):
    """
    Loads JSON data from S3 into Redshift using a COPY command.
    """

    ui_color = '#358140'

    # Allow templating for backfills
    template_fields = ("s3_key",)

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="redshift",
                 aws_conn_id="aws_credentials",
                 table=None,
                 s3_bucket=None,
                 s3_key=None,
                 json_path="auto",
                 region="us-west-2",
                 truncate_table=True,
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)

        if not table:
            raise ValueError("StageToRedshiftOperator requires 'table'")
        if not s3_bucket:
            raise ValueError("StageToRedshiftOperator requires 's3_bucket'")
        if not s3_key:
            raise ValueError("StageToRedshiftOperator requires 's3_key'")

        self.redshift_conn_id = redshift_conn_id
        self.aws_conn_id = aws_conn_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.json_path = json_path
        self.region = region
        self.truncate_table = truncate_table

    def execute(self, context):
        self.log.info("Starting staging for table %s", self.table)

        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        aws_hook = AwsBaseHook(aws_conn_id=self.aws_conn_id, client_type="sts")
        credentials = aws_hook.get_credentials()

        if self.truncate_table:
            self.log.info("Truncating table %s", self.table)
            redshift.run(f"TRUNCATE TABLE {self.table};")

        s3_path = f"s3://{self.s3_bucket}/{self.s3_key}"

        copy_sql = f"""
            COPY {self.table}
            FROM '{s3_path}'
            ACCESS_KEY_ID '{credentials.access_key}'
            SECRET_ACCESS_KEY '{credentials.secret_key}'
            REGION '{self.region}'
            FORMAT AS JSON '{self.json_path}'
            TIMEFORMAT AS 'epochmillisecs'
            TRUNCATECOLUMNS
            BLANKSASNULL
            EMPTYASNULL;
        """

        self.log.info("Running COPY command for table %s", self.table)
        redshift.run(copy_sql)
        self.log.info("Staging complete for table %s", self.table)
