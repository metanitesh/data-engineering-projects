from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers import SqlQueries

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = '',
                 query = '',
                 truncate = False,
                 table = '',
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.query = query
        self.truncate = truncate
        self.table = table

    def execute(self, context):
        self.log.info('LoadDimensionOperator starts')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        if(self.truncate):
            self.log.info('Truncating table')
            redshift.run("TRUNCATE TABLE {}".format(self.table))
        

        self.log.info('Running query on redshift')
        redshift.run(getattr(SqlQueries,self.query))
        self.log.info('Query successfully completed')

