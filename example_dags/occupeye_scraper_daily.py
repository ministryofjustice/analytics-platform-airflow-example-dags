from airflow.utils.dates import days_ago
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.models import DAG
from datetime import datetime, timedelta

log = LoggingMixin().log

try:
    from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator

    args = {"owner": "airflow", "start_date": datetime(2017, 1, 1), "retries": 5, "retry_delay": timedelta(minutes=5), "pool": "occupeye_pool"}

    dag = DAG(
        dag_id="occupeye_scraper_daily",
        default_args=args,
        schedule_interval='0 3 * * *',
    )

    surveys_to_s3 = KubernetesPodOperator(
        namespace="airflow",
        image="robinlinacre/airflow-occupeye-scraper:v4",
        cmds=["bash", "-c"],
        arguments=["python main.py --scrape_type=daily --scrape_datetime={{ts}}"],
        labels={"foo": "bar"},
        name="airflow-test-pod",
        in_cluster=True,
        task_id="scrape_all",
        get_logs=True,
        dag=dag,
        annotations={"iam.amazonaws.com/role": "dev_ravi_test_airflow_assume_role"},
    )

except ImportError as e:
    log.warn("Could not import KubernetesPodOperator: " + str(e))

