# Airflow Example DAGs


> Example DAGs for users to refer to when writing their own

These are example DAGs for apache-airflow 

### What is a DAG
In Airflow, a `DAG`—or a Directed Acyclic Graph—is a collection of all the tasks you want to run, organized in a way that reflects their relationships and dependencies.

### Give a DAG AWS permissions to do things on my behalf?
You can pass a kube2iam annotation to the PodOperator see the [assume role operator][kube2iam_example] for details.

### Run a AWS Glue job

> TODO

## More reading
https://airflow.apache.org/concepts.html

## Contribute

PRs accepted.


## License
 Apache 2.0
 © Ministry of Justice

[kube2iam_example]: example_dags/example_kubernetes_assume_role.py