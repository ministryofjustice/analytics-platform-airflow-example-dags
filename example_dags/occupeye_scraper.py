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

    surveys_to_xcom = KubernetesPodOperator(
        namespace="airflow",
        image="robinlinacre/airflow-occupeye-scraper:second",
        cmds=["bash", "-c"],
        arguments=["python main.py --task_name surveys_to_xcom"],
        labels={"foo": "bar"},
        name="airflow-test-pod",
        in_cluster=True,
        task_id="surveys_to_xcom",
        get_logs=True,
        dag=dag,
        annotations={"iam.amazonaws.com/role": "dev_ravi_test_airflow_assume_role"}
    )  #Can't annotate so won't have s3 perm

    # surveys_to_s3 = KubernetesPodOperator(
    #     namespace="airflow",
    #     image="robinlinacre/airflow-occupeye-scraper:second",
    #     cmds=["bash", "-c"],
    #     arguments=["python main.py --task_name surveys_to_s3"],
    #     labels={"foo": "bar"},
    #     name="airflow-test-pod",
    #     in_cluster=True,
    #     task_id="surveys_to_s3",
    #     get_logs=True,
    #     dag=dag,
    #     annotations={"iam.amazonaws.com/role": "dev_ravi_test_airflow_assume_role"},
    # )

    # surveys_to_xcom >> surveys_to_s3

    # surveys_to_s3

    # v1 = surveys_to_xcom.xcom_pull(key='python_set', task_ids='push')


except ImportError as e:
    log.warn("Could not import KubernetesPodOperator: " + str(e))
