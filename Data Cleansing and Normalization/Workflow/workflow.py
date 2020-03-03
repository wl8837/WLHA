import datetime
from airflow import models
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator

default_dag_args = {
        # https://airflow.apache.org/faq.html#what-s-the-deal-with-start-date}
        'start_date':datetime.datetime(2019,4,29)
}

###### Beam variables ######
LOCAL_MODE = 1  # run beam jobs locally
DIST_MODE = 2  # run beam jobs on Dataflow

mode = LOCAL_MODE

if mode == LOCAL_MODE:
    duration_script = 'duration_single.py'
    time_script = 'transform_time2_single.py'

if mode == DIST_MODE:
    duration_script = 'duration_cluster.py'
    time_script = 'transform_time2_cluster.py'

#### create table in new dataset

raw_dataset = 'dataset2'
new_dataset = 'workflow'
sql_cmd_start = 'bq query --use_legacy_sql = false'

sql_time = 'create table' + new_dataset + '.time as ' \
        'select event_id, begin_date_time as new_begin_date, end_date_time as new_end_date, year, month_name' \
        'from' + raw_dataset + '.storm2009' \
        'union distinct ' \
        'select event_id, begin_date_time as new_begin_date, end_date_time as new_end_date, year, month_name' \
        'from' + raw_dataset + '.storm2010' \
        'union distinct ' \
        'select event_id, begin_date_time as new_begin_date, end_date_time as new_end_date, year, month_name' \
        'from' + raw_dataset + '.storm2011' \
        'union distinct ' \
        'select event_id, begin_date_time as new_begin_date, end_date_time as new_end_date, year, month_name' \
        'from' + raw_dataset + '.storm2012' \
        'union distinct ' \
        'select event_id, begin_date_time as new_begin_date, end_date_time as new_end_date, year, month_name' \
        'from' + raw_dataset + '.storm2013' \

sql_storm = 'create table' + new_dataset + '.storm_event as' \
        'select event_id, cz_name, begin_date_time, begin_date_time, state' \
        'from' + raw_dataset + '.storm2009' \
        'union distinct ' \
        'select event_id, cz_name, begin_date_time, begin_date_time, state' \
        'from' + raw_dataset + '.storm2010' \
        'union distinct ' \
        'select event_id, cz_name, begin_date_time, begin_date_time, state' \
        'from' + raw_dataset + '.storm2011' \
        'union distinct ' \
        'select event_id, cz_name, begin_date_time, begin_date_time, state' \
        'from' + raw_dataset + '.storm2012' \
        'union distinct ' \
        'select event_id, cz_name, begin_date_time, begin_date_time, state' \
        'from' + raw_dataset + '.storm2013' \

with models.DAG (
        'workflow',
        schedule_interval = datetime.timedelta (days = 1),
        default_args = default_dag_args) as dag:

    ####### sql tasks #######
    delete_dataset = BashOperator(
        task_id='delete_dataset',
        bash_command='bq rm -r -f workflow')

    create_dataset = BashOperator (
        task_id = 'create_dataset',
        bash_command = 'bq mk workflow')

    create_time_table = BashOperator(
        task_id = 'create_time_table',
        bash_command = sql_cmd_start + '"' + sql_time + '"')

    create_storm_table = BashOperator(
        task_id='create_time_table',
        bash_command=sql_cmd_start + '"' + sql_storm + '"')

    transform_time = BashOperator(
        task_id = 'transform_time',
        bash_command = 'python /home/anhyejin0714/.local/bin/dags' + time_script)

    transform_time2 = BashOperator (
        task_id = 'transform_time2',
        bash_command = 'python /home/anhyejin0714/.local/bin/dags' + duration_script)

    transition = DummyOperator(task_id='transition')

    delete_dataset >> create_dataset >> [create_time_table, create_storm_table] >> transition >> transform_time >> transform_time2
