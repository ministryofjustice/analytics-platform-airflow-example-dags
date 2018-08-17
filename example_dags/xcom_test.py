import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

args = {
    'owner': 'airflow',
    'start_date': airflow.utils.dates.days_ago(2),
    'provide_context': True
}

dag = DAG(
    'robin_xcom',
    schedule_interval=None,
    default_args=args)


import pprint
def push(**kwargs):
    # ti is task instance
    pprint.pprint(kwargs)
    kwargs['ti'].xcom_push(key='python_set', value=set(['a', 'b']))


def puller(**kwargs):
    ti = kwargs['ti']

    # get value_1
    v1 = ti.xcom_pull(key='python_set', task_ids='push')
    print('The following value was pulled from xcom')
    print(v1)

push1 = PythonOperator(
    task_id='push', dag=dag, python_callable=push)

pull = PythonOperator(
    task_id='puller', dag=dag, python_callable=puller)

pull.set_upstream(push1)