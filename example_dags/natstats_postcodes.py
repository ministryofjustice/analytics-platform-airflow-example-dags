from airflow.utils.dates import days_ago
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.models import DAG
from datetime import datetime, timedelta

log = LoggingMixin().log

SCRAPER_IMAGE = "quay.io/mojanalytics/airflow_natstats_postcodes:latest"
SCRAPER_IAM_ROLE = "airflow_natstats_postcodes"


try:
    from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator

    args = {"owner": "Robin",
            "start_date": days_ago(0),  # No point in backfilling/catchup as only latest data is available
            "retries": 2,
            "retry_delay": timedelta(minutes=120),
            "email": ["robin.linacre@digital.justice.gov.uk"]}

    dag = DAG(
        dag_id="natstats_postcodes",
        default_args=args,
        schedule_interval='@hourly',
    )
    # https://github.com/apache/incubator-airflow/blob/5a3f39913739998ca2e9a17d0f1d10fccb840d36/airflow/contrib/operators/kubernetes_pod_operator.py#L129
    surveys_to_s3 = KubernetesPodOperator(
        namespace="airflow",
        image=SCRAPER_IMAGE,
        image_pull_policy='Always',
        cmds=["bash", "-c"],
        arguments=["python main.py"],
        labels={"foo": "bar"},
        name="airflow-test-pod",
        in_cluster=True,
        task_id="natstats_postcodes_task",
        get_logs=True,
        dag=dag,
        annotations={"iam.amazonaws.com/role": SCRAPER_IAM_ROLE},
    )

except ImportError as e:
    log.warn("Could not import KubernetesPodOperator: " + str(e))

