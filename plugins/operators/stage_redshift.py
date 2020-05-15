from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    @apply_defaults
    def __init__(
            self,
            redshift_conn_id='',
            aws_credentials_id='',
            table='',
            s3_bucket='',
            s3_key='',
            json_path='',
            file_type='',
            delimiter=',',
            ignore_headers=1,
            *args, **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.json_path = json_path
        self.file_type = file_type
        self.delimiter = delimiter
        self.ignore_headers = ignore_headers

    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.conn_id, schema='dev')

        self.log.info('Starting the copy process')
        s3_formatted_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, s3_formatted_key)

        print('s3 here ', s3_formatted_key)
        print('path here ', s3_path)

        sql = None
        if self.file_type == 'json':
            sql = f"""
                COPY {self.table}
                FROM '{s3_path}'
                ACCESS_KEY_ID '{credentials.access_key}'
                SECRET_ACCESS_KEY '{credentials.secret_key}'
                JSON '{self.json_path}'
                REGION 'us-west-2'
                COMPUPDATE OFF
            """
        elif self.file_type == 'csv':
            sql = f"""
                COPY {self.table}
                FROM '{s3_path}'
                ACCESS_KEY_ID '{credentials.access_key}'
                SECRET_ACCESS_KEY '{credentials.secret_key}'
                IGNOREHEADER {self.ignore_headers}
                REGION 'us-west-2'
                DELIMITER '{self.delimiter}'
            """
        redshift.run(sql)
