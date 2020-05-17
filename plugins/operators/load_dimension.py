from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadDimensionOperator(BaseOperator):
    """
    Loads data into dimension tables of Star Schema
    """
    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 table,
                 redshift_conn_id,
                 load_sql,
                 delete_load=True,
                 *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.conn_id = redshift_conn_id
        self.table = table
        self.load_sql = load_sql
        self.delete_load: bool = delete_load

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.conn_id)
        self.log.info(f'Loading dimension table {self.table} in Redshift')

        sql = f"""
            BEGIN
            TRUNCATE TABLE {self.table}
            INSERT INTO {self.table}
            {self.load_sql};
            COMMIT;
        """
        if not self.delete_load:
            sql = f"""
                BEGIN
                INSERT INTO {self.table}
                {self.load_sql};
                COMMIT;
            """
        redshift.run(sql)
