from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.secrets.metastore import MetastoreBackend
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):
    drop_table= """
        TRUNCATE TABLE IF EXISTS {}
        """
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 insert_sql="",
                 create_sql="",
                 table="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id=redshift_conn_id
        self.create_sql=create_sql
        self.insert_sql = insert_sql
        self.table=table

    def execute(self, context):
        metastoreBackend = MetastoreBackend()
        redshift_hook = PostgresHook(self.redshift_conn_id)
        redshift_hook.run(f"TRUNCATE TABLE IF EXISTS {self.table}")
        redshift_hook.run(self.create_sql)
        
        self.log.info('Fact table was created')

        custom_sql= self.insert_sql
        redshift_hook.run(custom_sql)

        self.log.info(f"Data inserted into {self.table}")


  
