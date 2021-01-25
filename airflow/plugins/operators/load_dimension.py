from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 dimension_table="",
                 select_sql_stmt="",
                 reload=False,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id,
        self.dimension_table = dimension_table,
        self.select_sql_stmt = select_sql_stmt
        self.reload = reload


    def execute(self, context):
        self.log.info(f"dimension_table: {self.dimension_table}, redshift_conn_id: {self.redshift_conn_id}")

        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        insert_sql_stmt = f"""
            insert into {self.dimension_table}
            {self.select_sql_stmt}
        """

        self.log.debug(f"insert_sql_stmt: {insert_sql_stmt}")

        if self.reload:
            self.log.info(f"truncating dimension table {self.dimension_table} on redshift before loading it because reload=True")
            redshift.run(f"truncate table {self.dimension_table};")

        self.log.info(f"Starting to load dimension table {self.dimension_table} on redshift")
        redshift.run(insert_sql_stmt)
        self.log.info(f"Done loading fact table {self.dimension_table} on redshift")