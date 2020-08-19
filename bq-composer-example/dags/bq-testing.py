import datetime
import logging
import airflow
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.operators.email_operator import EmailOperator
from airflow.operators import python_operator
from os.path import join

YESTERDAY = datetime.datetime.now() - datetime.timedelta(days=1)
HOME_PATH = '/home/airflow/gcs/dags'
CONFIG_FOLDER = 'conf/bq-testing.properties'
config_data = eval(open(join(HOME_PATH, CONFIG_FOLDER)).read())
default_args = config_data.get('default_args')

default_args['retry_delay'] = datetime.timedelta(
    minutes=default_args.get('retry_delay_minute'))
default_args['start_date'] = datetime.datetime.now() - datetime.timedelta(
    days=default_args.get('start_date_delay'))

with airflow.DAG(
        dag_id='bq-testing',
        catchup=False,
        default_args=default_args,
        params=config_data.get('query-parameters'),
        schedule_interval=None) as dag:

    # Print the dag_run id from the Airflow logs
    # print_dag_run_conf = bash_operator.BashOperator(
    #     task_id='print_dag_run_conf', bash_command='echo {{ dag_run.id }}')
    # mergeQury = """
    # CREATE OR REPLACE TABLE `sublime-mission-251813.practice_data_1.sensor_data_testing`
    # AS
    # SELECT
    #     *
    # FROM
    #     `sublime-mission-251813.practice_data_1.sensor_data`
    # WHERE
    #     1=1
    #     --AND update_time > '{{ prev_start_date_success }}' -- returning null
    #     -- AND update_time < '{{ ts }}' -- for first time
    #     AND update_time < TIMESTAMP_SUB('{{ ts }}', INTERVAL 5 MINUTE)
    # ;
    # """

    def log_stack(data="Test-signal"):
        logging.info("{}".format(data))

    mergeTask = BigQueryOperator(
       task_id='test-query',
       bql='scripts/bq-testing.sql',
       use_legacy_sql=False
       )

    start_email = EmailOperator(
       task_id='start-email',
       to='am0072008@gmail.com',
       subject='execution of dag:{{dag_id}} started',
       html_content='Hello'
       )

    init_log = python_operator.PythonOperator(
        task_id='init_log',
        python_callable=log_stack,
        op_kwargs={'data': '-----------------Begin-------------------------'}
    )

    end_log = python_operator.PythonOperator(
        task_id='end_log',
        python_callable=log_stack,
        op_kwargs={'data': '-----------------End-------------------------'}
    )

    para1_log = python_operator.PythonOperator(
        task_id='PARA1_log',
        python_callable=log_stack,
        op_kwargs={'data': '-----------------Para1-------------------------'}
    )

    para2_log = python_operator.PythonOperator(
        task_id='PARA2_log',
        python_callable=log_stack,
        op_kwargs={'data': '-----------------Para2-------------------------'}
    )

    init_log >> start_email >> mergeTask >> end_log
    start_email >> para1_log
    mergeTask >> para2_log
