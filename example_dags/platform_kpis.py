from airflow.utils.dates import days_ago
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.models import DAG
from datetime import datetime, timedelta

log = LoggingMixin().log

SCRAPER_IMAGE = "quay.io/airflow-elasticsearch-kpis:latest"
SCRAPER_IAM_ROLE = "dev_pq_scraper_dag"


try:
    from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator

    args = {"owner": "Robin",
            "start_date": days_ago(0),
            "retries": 3,
            "retry_delay": timedelta(minutes=50),
            "email": ["robin.linacre@digital.justice.gov.uk"]}

    dag = DAG(
        dag_id="platform_kpi_scraper",
        default_args=args,
        schedule_interval='0 1 * * *',
    )

    surveys_to_s3 = KubernetesPodOperator(
        namespace="airflow",
        image=SCRAPER_IMAGE,
        cmds=["bash", "-c"],
        arguments=["python main.py"],
        labels={"foo": "bar"},
        name="airflow-test-pod",
        in_cluster=True,
        task_id="get_kpis",
        get_logs=True,
        dag=dag,
        annotations={"iam.amazonaws.com/role": SCRAPER_IAM_ROLE},
    )



except ImportError as e:
    log.warn("Could not import KubernetesPodOperator: " + str(e))

