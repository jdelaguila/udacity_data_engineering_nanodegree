from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 table='',
                 sql_query='',
                 append=True,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)   
        self.redshift_conn_id=redshift_conn_id
        self.table=table
        self.sql_query=sql_query
        self.append=append

    def execute(self, context):
        self.log.info('connecting to redshift')
        redshift=PostgresHook(self.redshift_conn_id)
        self.log.info(f'select records from s3, insert into {self.table}')
        if not self.append:
            self.log.info(f'deleting table {self.table}')
            redshift.run(f'TRUNCATE {self.table}')
        self.log.info(f'inserting data to {self.table}')
        redshift.run(f'INSERT INTO {self.table} {self.sql_query}')
