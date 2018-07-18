from datetime import datetime, timedelta

import airflow
from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.utils.dates import days_ago


SCRAPER_IMAGE = "quay.io/mojanalytics/pq_scraper:v0.1.2"
SCRAPER_IAM_ROLE = "dev_pq_scraper_dag"
SCRAPER_S3_BUCKET = "dev-aldo-test-20170927-1505"
SCRAPER_S3_OBJECT_PREFIX = "pq_raw/answered_questions_"

CATCHUP_START = datetime(2018, 2, 1)
REUPDATE_LAST_N_DAYS = 31


task_args = {
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 15,
    "retry_delay": timedelta(seconds=30),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=15),
    "execution_timeout": timedelta(minutes=10),
    "owner": "xoen",
    "email": ["aldogiambelluca@digital.justice.gov.uk"],
}

# Catch-up on dates before today-REUPDATE_LAST_N_DAYS days
dag_catchup = DAG(
    "pq_scraper_catchup",
    default_args=task_args,
    description="Get answered parliamentary questions (PQ) from parliament.uk API",
    start_date=CATCHUP_START,
    end_date=days_ago(REUPDATE_LAST_N_DAYS - 1),
    schedule_interval=timedelta(days=1),
)

task_catchup = KubernetesPodOperator(
    dag=dag_catchup,
    namespace="airflow",
    image=SCRAPER_IMAGE,
    env_vars={
        "SCRAPER_S3_BUCKET": SCRAPER_S3_BUCKET,
        "SCRAPER_S3_OBJECT_PREFIX": SCRAPER_S3_OBJECT_PREFIX,
    },
    arguments=["{{ ds }}"],
    labels={"app": dag_catchup.dag_id},
    name="pq-scraper-catchup",
    in_cluster=True,
    task_id="pq_scrape_catchup",
    get_logs=True,
    annotations={"iam.amazonaws.com/role": SCRAPER_IAM_ROLE},
)

# Update last REUPDATE_LAST_N_DAYS days
dag_recent_pqs = DAG(
    "pq_scraper_recent",
    start_date=days_ago(1),
    schedule_interval=timedelta(days=1),
    default_args=task_args,
    description="Get answered parliamentary questions (PQ) from parliament.uk API",
)

for days_ago in range(0, REUPDATE_LAST_N_DAYS + 1):
    KubernetesPodOperator(
        dag=dag_recent_pqs,
        namespace="airflow",
        image=SCRAPER_IMAGE,
        env_vars={
            "SCRAPER_S3_BUCKET": SCRAPER_S3_BUCKET,
            "SCRAPER_S3_OBJECT_PREFIX": SCRAPER_S3_OBJECT_PREFIX,
        },
        arguments=["{{ macros.ds_add(ds, -params.days_ago) }}"],
        params={"days_ago": days_ago},
        labels={"app": dag_recent_pqs.dag_id},
        name="pq-scraper-catchup",
        in_cluster=True,
        task_id=f"pq_scrape_recent_{days_ago}_days_ago",
        get_logs=True,
        annotations={"iam.amazonaws.com/role": SCRAPER_IAM_ROLE},
    )
