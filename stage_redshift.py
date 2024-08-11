from airflow.secrets.metastore import MetastoreBackend
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class StageToRedshiftOperator(BaseOperator):
    #template_fields = ("s3_key",)
    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        FORMAT AS {}
        TIMEFORMAT AS 'epochmillisecs' 
        COMPUPDATE OFF;
    """

    drop_table= """
        DROP TABLE IF EXISTS {}
        """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_path="",
                 json_path="",
                 create_staging_table_sql="",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.s3_path = s3_path
        self.json_path=json_path
        self.create_staging_table_sql=create_staging_table_sql
        self.aws_credentials_id = aws_credentials_id

    def execute(self, context):
        metastoreBackend = MetastoreBackend()
        aws_connection=metastoreBackend.get_connection(self.aws_credentials_id)
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        redshift_hook.run(StageToRedshiftOperator.drop_table.format(self.table)) 
        self.log.info("Dropping destination Redshift table if exists")

        redshift_hook.run(self.create_staging_table_sql)
        self.log.info("Creating staging table")

        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table, self.s3_path, aws_connection.login,
            aws_connection.password, self.json_path)

        redshift_hook.run(formatted_sql)
        
        
