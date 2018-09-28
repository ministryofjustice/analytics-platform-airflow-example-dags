from datetime import datetime, timedelta

import airflow
from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.utils.dates import days_ago


FLATTENER_IMAGE = "quay.io/mojanalytics/pq_flattener:v1.0.0"
FLATTENER_IAM_ROLE = "dev_pq_flattener"

FLATTENER_GLUE_JOB_BUCKET = "dev-cds-curated-open-data"
FLATTENER_SOURCE_PATH = f"s3://dev-cds-raw/open_data/parliamentary_questions/"
FLATTENER_DEST_PATH = f"s3://dev-cds-curated-open-data/parliamentary_questions/"

START_DATE = datetime(2018, 8, 15)


task_args = {
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 15,
    "retry_delay": timedelta(seconds=30),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=15),
    "execution_timeout": timedelta(minutes=10),
    "owner": "xoen",
    "email": ["aldogiambelluca@digital.justice.gov.uk"],
}

dag_flattener = DAG(
    "pq_flattener",
    default_args=task_args,
    description="Aggregate parliamentary questions (PQ) raw JSON data into a single, flat, CSV file",
    start_date=START_DATE,
    catchup=False,
    schedule_interval=timedelta(days=1),
)

task_flattener = KubernetesPodOperator(
    dag=dag_flattener,
    namespace="airflow",
    image=FLATTENER_IMAGE,
    env_vars={
        "PQ_FLATTENER_GLUE_JOB_BUCKET": FLATTENER_GLUE_JOB_BUCKET,
        "PQ_FLATTENER_SOURCE_PATH": FLATTENER_SOURCE_PATH,
        "PQ_FLATTENER_DEST_PATH": FLATTENER_DEST_PATH,
        "PQ_FLATTENER_JOB_IAM_ROLE": FLATTENER_IAM_ROLE,
    },
    arguments=["{{ ds }}"],
    labels={"app": dag_flattener.dag_id},
    name="pq-flattener",
    in_cluster=True,
    task_id="pq_flattener",
    get_logs=True,
    annotations={"iam.amazonaws.com/role": FLATTENER_IAM_ROLE},
)
