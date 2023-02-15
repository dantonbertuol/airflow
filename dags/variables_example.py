from airflow import DAG
from airflow.operators.base_operator import BaseOperator
import datetime as dt

with DAG(dag_id="variables_example",
         start_date=dt.datetime(2020, 1, 1),
         schedule_interval=None
         ) as dag:

    t1 = BaseOperator(
        task_id="t1",
        bash_command="echo {{ var.value.volume_temp }}"  # obtem o valor da variável volume_temp
    )

t1
