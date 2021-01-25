from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 check_list=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.check_list = check_list


    def do_check(self, connection, name, sql_stmt, expected_result, comparator):

        check_outcome = False

        try:
            self.log.info(f"executing sql: {sql_stmt}, expecting result: {expected_result}, comparator: {comparator}")
            result = connection.get_records(sql_stmt)[0]

            if comparator == "eq":
                self.log.debug(f"checking for {result} to be equal-to {expected_result}")
                if result == expected_result:
                    check_outcome = True
                    self.log.debug(f"check passed")
                else:
                    check_outcome = False
                    self.log.debug(f"check failed")


            elif comparator == "ne":
                self.log.debug(f"checking for {result} to be not-equal-to {expected_result}")
                if result != expected_result:
                    check_outcome = True
                    self.log.debug(f"check passed")
                else:
                    check_outcome = False
                    self.log.debug(f"check failed")

            elif comparator == "le":
                self.log.debug(f"checking for {result} to be less-than-or-equal-to {expected_result}")
                if result <= expected_result:
                    check_outcome = True
                    self.log.debug(f"check passed")
                else:
                    check_outcome = False
                    self.log.debug(f"check failed")

            elif comparator == "ge":
                self.log.debug(f"checking for {result} to be greater-than-or-equal-to {expected_result}")
                if result >= expected_result:
                    check_outcome = True
                    self.log.debug(f"check passed")
                else:
                    check_outcome = False          
                    self.log.debug(f"check failed")  
            
            elif comparator == "gt":
                self.log.debug(f"checking for {result} to be greater-than {expected_result}")
                if result > expected_result:
                    check_outcome = True
                    self.log.debug(f"check passed")
                else:
                    check_outcome = False
                    self.log.debug(f"check failed")
            
            elif comparator == "lt":
                self.log.debug(f"checking for {result} to be less-than {expected_result}")
                if result < expected_result:
                    check_outcome = True
                    self.log.debug(f"check passed")
                else:
                    check_outcome = False
                    self.log.debug(f"check failed")
            
            else:
                self.log.debug("comparator {comparator} did not match any of the checks")
                check_outcome = False

        except Exception as e:
            self.log.error(f"Exeception performing data quality check {e}")
            return False

        return check_outcome


    def execute(self, context):
        self.log.info(f"redshift_conn_id: {self.redshift_conn_id}")

        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        failed_tests = []
        fail_count = 0

        for check in self.check_list:
            
            check_name = check['name']

            check_outcome = self.do_check(connection = redshift, 
                                    name = check_name,
                                    sql_stmt = check['sql_stmt'], 
                                    expected_result = check['expected_result'], 
                                    comparator = check['comparator']
                                    )
            if check_outcome:
                self.log.info(f"check {check_name} passed.")
            else:
                self.log.info(f"check {check_name} failed.")
                failed_tests.append(check_name)
                fail_count = fail_count + 1

        
        if fail_count > 0:
            self.log.error(f"{fail_count} data qualifty checks failed:")
            self.log.error(f"{failed_tests}")
            raise ValueError(f"{fail_count} data qualifty checks failed: {failed_tests}")
        else:
            self.log.info("Looks like all data quality checks passed")
