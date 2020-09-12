from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 redshift_conn_id="",
                 dq_checks=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.checks=dq_check
        self.redshift_conn_id = redshift_conn_id
        
        # Map params here
        # Example:
        # self.conn_id = conn_id

    def execute(self, context):
        self.log.info('DataQualityOperator not implemented yet')
        
        
        for check in dq_checks:
            sql = checks["check_sql"]
            exp = check["expected-result"]
            record = redshift_hook.get_records(sql)
            num_record = record[0][0]
            if num_record != exp:
                raise ValueError(f"Data quality check failed. Expected: {exp} | Got: {num_records}")          
            else:
                self.log.info(f"Data quality on SQL {sql} check passed with {records[0][0]} records")