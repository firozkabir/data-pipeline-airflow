from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 region="us-west-2",
                 extra_params="",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id=redshift_conn_id,
        self.aws_credentials_id=aws_credentials_id,
        self.table=table,
        self.s3_bucket=s3_bucket,
        self.s3_key=s3_key,
        self.region=region,
        self.extra_params = extra_params



    def execute(self, context):
        
        self.log.info(f"getting aws_ceredential_id: {aws_credentials_id} and redshift_conn_id: {redshift_conn_id}")
        aws_hook = AwsHook(self.aws_credentials_id)
        aws_credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        


        s3_path = f"s3://{self.s3_bucket}/{self.s3_key}"

        

        redshift_copy_sql = f"""
            copy {self.table}
            from '{s3_path}'
            access_key_id '{aws_credentials.access_key}'
            secret_access_key '{aws_credentials.secret_key}'
            region as '{self.region}'
            {self.extra_params}
            ;
        """

        self.log.debug(f"redshift_copy_sql: {redshift_copy_sql}")
        self.log.info(f"copying from s3 bucket {s3_path} to table {self.table}")
        redshift.run(redshift_copy_sql)
        self.log.info(f"DONE copying from s3 bucket {s3_path} to table {self.table}")











