from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="users",
                 sql= '',
                 column = '',
                 expected_result = 0,
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.sql = sql
        self.column = column
        self.expected_result = expected_result

    def execute(self, context):
        redshift_hook = PostgresHook(self.redshift_conn_id)
        records = redshift_hook.get_records(self.sql.format(self.column, self.table, self.column))
        if len(records) < 1 or len(records[0]) < 1:
            raise ValueError(f"Data quality check failed. {self.table} returned no results")
        num_records = records[0][0]
        if num_records != self.expected_result:
            raise ValueError(f"Data quality check failed. {self.table} contained {records[0][0]} rows")
        self.log.info(f"Data quality on table {self.table} check passed with {records[0][0]} records")

