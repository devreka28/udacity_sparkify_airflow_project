from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                redshift_conn_id='',
                create_sql='',
                insert_sql='',
                table='',
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id=redshift_conn_id
        self.create_sql=create_sql
        self.insert_sql=insert_sql
        self.table=table

    def execute(self, context):
        self.log.info('LoadDimensionOperator not implemented yet')

        redshift_hook=PostgresHook(self.redshift_conn_id)
        redshift_hook.run(f"TRUNCATE TABLE IF EXISTS {self.table}")
        
        redshift_hook.run(self.create_sql)

        self.log.info(f'{self.table} table was created.')

        redshift_hook.run(self.insert_sql)

        self.log.info(f'Data was inserted into {self.table}')

