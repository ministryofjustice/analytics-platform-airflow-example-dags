from airflow.utils.dates import days_ago
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.models import DAG

log = LoggingMixin().log

try:
    from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator

    args = {"owner": "airflow", "start_date": days_ago(3)}

    dag = DAG(
        dag_id="occupeye_scraper",
        default_args=args,
        schedule_interval='@daily',
    )

    # Hard code surveys list due to contstrains
    survey_ids = [340, 339, 338, 337, 336, 334, 333, 330, 327, 326, 324, 323, 322, 320, 319, 318, 317, 312, 308, 307, 306, 303, 300, 299, 297, 293, 278, 273, 272, 271, 270, 228, 227, 225, 181, 179, 177, 176, 174, 172, 166, 163, 162, 159, 156, 155, 154, 152, 151, 144, 139, 136, 131, 125, 124, 122, 121, 117, 115, 112, 111, 109, 107, 104, 103, 95, 94, 89, 86, 85, 84, 83, 80, 79, 73, 71, 65, 63, 61, 52, 51, 50, 49, 43, 41, 39, 38, 37, 31, 30, 29, 28, 19, 17, 16]
    # surveys_to_xcom = KubernetesPodOperator(
    #     namespace="airflow",
    #     image="robinlinacre/airflow-occupeye-scraper:second",
    #     cmds=["bash", "-c"],
    #     arguments=["python main.py --task_name surveys_to_xcom"],
    #     labels={"foo": "bar"},
    #     name="airflow-test-pod",
    #     in_cluster=True,
    #     task_id="surveys_to_xcom",
    #     get_logs=True,
    #     dag=dag,
    #     annotations={"iam.amazonaws.com/role": "dev_ravi_test_airflow_assume_role"},
    #     xcom_push=True
    # )  #Can't annotate so won't have s3 perm

    surveys_to_s3 = KubernetesPodOperator(
        namespace="airflow",
        image="robinlinacre/airflow-occupeye-scraper:third",
        cmds=["bash", "-c"],
        arguments=["python main.py --task_name surveys_to_s3"],
        labels={"foo": "bar"},
        name="airflow-test-pod",
        in_cluster=True,
        task_id="surveys_to_s3",
        get_logs=True,
        dag=dag,
        annotations={"iam.amazonaws.com/role": "dev_ravi_test_airflow_assume_role"},
    )

    for sid in survey_ids:
        sensor_fact_to_s3 = KubernetesPodOperator(
            namespace="airflow",
            image="robinlinacre/airflow-occupeye-scraper:third",
            cmds=["bash", "-c"],
            arguments=[f"python main.py --task_name=surveys_to_s3 --surveyid={sid} --scrape_date={{ds}}"],
            labels={"foo": "bar"},
            name="airflow-test-pod",
            in_cluster=True,
            task_id=f"surveys_to_s3_{sid}",
            get_logs=True,
            dag=dag,
            annotations={"iam.amazonaws.com/role": "dev_ravi_test_airflow_assume_role"},
        )

        surveys_to_s3 >> sensor_fact_to_s3




    # surveys_to_xcom >> surveys_to_s3

    # surveys_to_s3

    # v1 = surveys_to_xcom.xcom_pull(key='python_set', task_ids='push')


except ImportError as e:
    log.warn("Could not import KubernetesPodOperator: " + str(e))
