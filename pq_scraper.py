from datetime import datetime, timedelta

import airflow
from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.utils.dates import days_ago


PQ_SCRAPER_IMAGE = "quay.io/mojanalytics/pq_scraper:v0.1.2"
PQ_SCRAPER_IAM_ROLE = "dev_pq_scraper_dag"
UPDATE_LAST_N_DAYS = 5

default_args = {
    "owner": "xoen",
    "email": ["aldogiambelluca@digital.justice.gov.uk"],
    "schedule_interval": timedelta(days=1),
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 5,
    "retry_delay": timedelta(minutes=5),
}

# Catch-up on dates before today-UPDATE_LAST_N_DAYS days
dag_catchup = DAG(
    "pq_scraper_catchup",
    default_args={
        **default_args,
        "start_date": datetime(2018, 6, 1),
        "end_date": days_ago(UPDATE_LAST_N_DAYS + 1),
    },
    description="Get answered parliamentary questions (PQ) from parliament.uk API",
)

task_catchup = KubernetesPodOperator(
    dag=dag_catchup,
    namespace="airflow",
    image=PQ_SCRAPER_IMAGE,
    arguments=["{{ ds }}"],
    labels={"app": dag.dag_id},
    name="pq_scraper_catchup",
    in_cluster=True,
    task_id="scrape_{{ ds }}",
    get_logs=True,
    annotations={"iam.amazonaws.com/role": PQ_SCRAPER_IAM_ROLE},
)

# Update last UPDATE_LAST_N_DAYS days

dag_recent_pqs = DAG(
    "pq_scraper_recent",
    default_args={**default_args, "start_date": days_ago(UPDATE_LAST_N_DAYS)},
    description="Get answered parliamentary questions (PQ) from parliament.uk API",
)

for days_ago in range(1, UPDATE_LAST_N_DAYS):
    KubernetesPodOperator(
        dag=dag_recent_pqs,
        namespace="airflow",
        image=PQ_SCRAPER_IMAGE,
        params={"days_ago": days_ago},
        arguments=["{{ ds_add(ds, -params.days_ago) }}"],
        labels={"app": dag.dag_id},
        name="pq_scraper_catchup",
        in_cluster=True,
        task_id="scrape_{{ ds_add(ds, -params.days_ago) }}",
        get_logs=True,
        annotations={"iam.amazonaws.com/role": PQ_SCRAPER_IAM_ROLE},
    )
