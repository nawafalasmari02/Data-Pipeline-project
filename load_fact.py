from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadFactOperator(BaseOperator):
    """
    Loads data into a fact table in Redshift.
    Fact loads are typically append-only.
    """

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="redshift",
                 table=None,
                 sql=None,
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)

        if not table:
            raise ValueError("LoadFactOperator requires 'table'")
        if not sql:
            raise ValueError("LoadFactOperator requires 'sql'")

        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql = sql

    def execute(self, context):
        self.log.info("Loading fact table: %s", self.table)

        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        insert_sql = f"""
            INSERT INTO {self.table}
            {self.sql};
        """

        self.log.info("Running INSERT for fact table %s", self.table)
        redshift.run(insert_sql)

        self.log.info("Fact load complete for table: %s", self.table)
