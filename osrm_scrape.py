
from airflow.utils.dates import days_ago
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.models import DAG
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago

log = LoggingMixin().log

try:
    from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator

    args = {"owner": "Robin",
            "retries":0,
            "email": ["robin.linacre@digital.justice.gov.uk"],
            "start_date": days_ago(1)}

    dag = DAG(
        dag_id="osrm_scrape",
        default_args=args,
        schedule_interval='@once',
    )

    osrm_scrape = KubernetesPodOperator(
        namespace="airflow",
        image="593291632749.dkr.ecr.eu-west-1.amazonaws.com/airflow-osrm-scrape:v0.0.4",
        cmds=["bash", "-c"],
        arguments=["python main.py"],
        labels={"foo": "bar"},
        name="airflow-test-pod",
        in_cluster=True,
        task_id="scrape_all",
        get_logs=True,
        dag=dag,
        annotations={"iam.amazonaws.com/role": "airflow_osrm_scraper"},
        image_pull_policy='Always'
    )

    run_gluejob = KubernetesPodOperator(
        namespace="airflow",
        image="593291632749.dkr.ecr.eu-west-1.amazonaws.com/airflow-osrm-scrape:v0.0.4",
        cmds=["bash", "-c"],
        arguments=["python run_glue.py"],
        labels={"foo": "bar"},
        name="airflow-test-pod",
        in_cluster=True,
        task_id="scrape_all",
        get_logs=True,
        dag=dag,
        annotations={"iam.amazonaws.com/role": "airflow_osrm_scraper"},
        image_pull_policy='Always'
    )

osrm_scrape >> run_gluejob

except ImportError as e:
    log.warn("Could not import KubernetesPodOperator: " + str(e))

