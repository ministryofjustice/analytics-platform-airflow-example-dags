import airflow
from airflow.operators.bash_operator import BashOperator
from airflow.models import DAG
from datetime import timedelta


args = {"owner": "Aldo", "start_date": airflow.utils.dates.days_ago(7)}

dag = DAG(
    dag_id="example_bash_lunch_time",
    default_args=args,
    schedule_interval="30 12 * * *",
    dagrun_timeout=timedelta(minutes=60),
)

echo = 'echo "It\'s lunch time!"'
echo_task = BashOperator(task_id="lunch_time", bash_command=echo, dag=dag)

if __name__ == "__main__":
    dag.cli()
