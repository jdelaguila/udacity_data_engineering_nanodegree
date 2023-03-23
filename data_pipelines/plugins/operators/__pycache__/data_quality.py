from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 checks=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id=redshift_conn_id
        self.checks=checks

    def execute(self, context):
        redshift_hook=PostgresHook(self.redshift_conn_id)
        for check in self.checks:
            results=int(redshift_hook.get_first(sql=check['test_sql'])[0])
            
            #if equal
            if check['operation'] == '=':
                if results != check['expected_results']:
                    self.log.info('Check failed: {} does not equal {} when running {}'.format(results, check['expected_results'], check['test_sql']))
                    raise AssertionError('Check failed: {} does not equal {} when running {}'.format(results, check['expected_results'], check['test_sql']))
            
            #if not equal
            elif check['operation'] == '!=':
                if results == check['expected_results']:
                    self.log.info('Check failed: {} equals {} when running {}'.format(results, check['expected_results'], check['test_sql']))
                    raise AssertionError('Check failed: {} equals {} when running {}'.format(results, check['expected_results'], check['test_sql']))
        
            #if greater than
            elif check['operation'] == '>':
                if results <= check['expected_results']:
                    self.log.info('Check failed: {} not greater than {} when running {}'.format(results, check['expected_results'], check['test_sql']))
                    raise AssertionError('Check failed: {} not greater than {} when running {}'.format(results, check['expected_results'], check['test_sql']))
                    
            self.log.info('Passed check: {} {} {} when running {}'.format(results, check['operation'], check['expected_results'], check['test_sql']))