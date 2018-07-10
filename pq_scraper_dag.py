from datetime import timedelta

import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

# from pq_scraper.scraper import get_questions


default_args = {
    "owner": "xoen",
    "email": ["aldogiambelluca@digital.justice.gov.uk"],
    "start_date": airflow.utils.dates.days_ago(1),
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 5,
    "retry_delay": timedelta(seconds=30),
}

dag = DAG(
    "pq_scraper",
    default_args=default_args,
    description="Get answered parliamentary questions from parliament.uk API",
)


# def print_questions(date):
#     print(date)
#     questions = get_questions(date)
#     print(questions)


# task_get_questions = PythonOperator(
#     task_id="get_questions_1",
#     dag=dag,
#     python_callable=print_questions,
#     op_kwargs={"date": "{{ds}}"},
# )

def sleep_forever(seconds):
    import time
    time.sleep(seconds)

task_sleep_foreveret_questions = PythonOperator(
    task_id="sleep_forever",
    dag=dag,
    python_callable=sleep_forever,
    op_kwargs={"seconds": 99999999},
)
