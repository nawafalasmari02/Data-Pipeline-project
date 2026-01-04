from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadDimensionOperator(BaseOperator):
    """
    Loads data into a dimension table in Redshift.

    Supports:
    - truncate_insert=True  → TRUNCATE then INSERT
    - truncate_insert=False → Append-only INSERT
    """

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="redshift",
                 table=None,
                 sql=None,
                 truncate_insert=True,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)

        if not table:
            raise ValueError("LoadDimensionOperator requires 'table'")
        if not sql:
            raise ValueError("LoadDimensionOperator requires 'sql'")

        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql = sql
        self.truncate_insert = truncate_insert

    def execute(self, context):
        self.log.info("Loading dimension table: %s", self.table)

        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if self.truncate_insert:
            self.log.info("Truncating table %s before load", self.table)
            redshift.run(f"TRUNCATE TABLE {self.table};")

        insert_sql = f"""
            INSERT INTO {self.table}
            {self.sql};
        """

        self.log.info("Running INSERT for dimension table %s", self.table)
        redshift.run(insert_sql)

        self.log.info("Dimension load complete for table: %s", self.table)
