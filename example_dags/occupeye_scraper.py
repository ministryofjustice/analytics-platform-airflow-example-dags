from airflow.utils.dates import days_ago
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.models import DAG

log = LoggingMixin().log

try:
    from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator

    args = {"owner": "airflow", "start_date": days_ago(1)}

    dag = DAG(
        dag_id="occupeye_scraper",
        default_args=args,
        schedule_interval=None,
    )

    k = KubernetesPodOperator(
        namespace="airflow",
        image="robinlinacre/airflow-occupeye-scraper:firsttry",
        cmds=["python main.py --task_name surveys_to_s3"],
        arguments=[""],
        labels={"foo": "bar"},
        name="airflow-test-pod",
        in_cluster=True,
        task_id="task",
        get_logs=True,
        dag=dag,
        annotations={"iam.amazonaws.com/role": "dev_ravi_test_airflow_assume_role"},
    )


except ImportError as e:
    log.warn("Could not import KubernetesPodOperator: " + str(e))
