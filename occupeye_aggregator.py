from airflow.utils.dates import days_ago
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.models import DAG
from datetime import datetime, timedelta

log = LoggingMixin().log

try:
    from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator

    args = {"owner": "Robin",
            "start_date": datetime(2018, 10, 28),
            "retries": 2,
            "retry_delay": timedelta(minutes=50),
            "email": ["robin.linacre@digital.justice.gov.uk"],
            "pool": "occupeye_pool"}

    dag = DAG(
        dag_id="occupeye_aggregator",
        default_args=args,
        schedule_interval='0 5 * * *',
    )

    surveys_to_s3 = KubernetesPodOperator(
        namespace="airflow",
        image="quay.io/mojanalytics/airflow-occupeye-dashboard-aggregation:dockerise",
        cmds=["bash", "-c"],
        arguments=["Rscript main.R"],
        labels={"foo": "bar"},
        name="airflow-test-pod",
        in_cluster=True,
        task_id="scrape_all",
        get_logs=True,
        dag=dag,
        annotations={"iam.amazonaws.com/role": "airflow_occupeye_aggregator"},
        image_pull_policy='Always'
    )



except ImportError as e:
    log.warn("Could not import KubernetesPodOperator: " + str(e))

