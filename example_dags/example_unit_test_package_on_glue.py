import airflow
from airflow.operators.slack_operator import SlackAPIPostOperator
from airflow.operators.dummy_operator import DummyOperator

from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.models import DAG

from datetime import datetime, timedelta
from airflow.utils.dates import days_ago

from airflow.utils.trigger_rule import TriggerRule

import boto3

UNIT_TEST_IMAGE = "quay.io/mojanalytics/gluejobutils_unit_test:v0.0.1"
UNIT_TEST_IAM_ROLE = "airflow_gluejobutils_unit_test"

GLUEJOBUTILS_GLUE_JOB_BUCKET = "alpha-gluejobutils"

s3_resource = boto3.resource('s3')
obj = s3_resource.Object(GLUEJOBUTILS_GLUE_JOB_BUCKET, 'slack_token.txt')
slack_token = obj.get()['Body'].read().decode('utf-8')

task_args = {
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(seconds=60),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=15),
    "execution_timeout": timedelta(minutes=10),
    "owner": "karik",
    "email": ["karik.isichei@digital.justice.gov.uk"],
}


# DAG default arguments
default_args = {
    'owner': 'karik',
    'start_date': datetime.now()
}

# Define DAG
dag = DAG('example_gluejobutils_unit_test',
        default_args=default_args,
        description='Testing a kubes operator to run a quick unit test and then post answer on slack')

# KEEPING HERE FOR REF
# dag_flattener = DAG(
#     "pq_flattener",
#     default_args=task_args,
#     description="Aggregate parliamentary questions (PQ) raw JSON data into a single, flat, CSV file",
#     start_date=START_DATE,
#     catchup=False,
#     schedule_interval=timedelta(days=1),
# )

task_unit_test = KubernetesPodOperator(
    dag=dag,
    namespace="airflow",
    image=UNIT_TEST_IMAGE,
    env_vars={
        "GLUEJOBUTILS_GLUE_JOB_BUCKET": GLUEJOBUTILS_GLUE_JOB_BUCKET,
        "UNIT_TEST_IAM_ROLE" : UNIT_TEST_IAM_ROLE
    },
    arguments=["{{ ds }}"],
    labels={"app": dag.dag_id},
    name="glue-job-unit-test",
    in_cluster=True,
    task_id="glue_job_unit_test",
    get_logs=True,
    annotations={"iam.amazonaws.com/role": UNIT_TEST_IAM_ROLE}
)

slack_failed = SlackAPIPostOperator(
         task_id='slack_failed',
         channel="#airflow_alerts",
         token=slack_token,
         text = ':red_circle: DAG Failed',
         owner = 'karik',
         trigger_rule=TriggerRule.ONE_FAILED,
         dag=dag)

slack_passed = SlackAPIPostOperator(
         task_id='slack_passed',
         channel="#airflow_alerts",
         token=slack_token,
         text = ':green_circle: DAG SUCEEDED',
         owner = 'karik',
         trigger_rule=TriggerRule.ALL_SUCCESS,
         dag=dag)


task_unit_test >> slack_failed
task_unit_test >> slack_passed