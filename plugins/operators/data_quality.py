from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DataQualityOperator(BaseOperator):
    """
    Checks quality of data. Yields message should data exist or not.
    """
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 tables=[],
                 *args, **kwargs):

        super().__init__(*args, **kwargs)
        self.tables = tables
        self.conn_id = redshift_conn_id

    def execute(self, context):
        redshift_hook = PostgresHook(self.conn_id)
        for table in self.tables:
            self.log.info(f'Checking {table} table')
            records = redshift_hook.get_records(f"SELECT COUNT(*) FROM {table}")
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f'Quality check failed. {table} yielded no results')
            num_records = records[0][0]
            if num_records < 1:
                raise ValueError(f'Quality check failed. {table} contained no rows')
            self.log.info(f'Data quality on table {table} check passed with {records[0][0]} records')
