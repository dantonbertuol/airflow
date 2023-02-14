from airflow import DAG
from airflow.operators.bash import BashOperator
import datetime as dt

dag = DAG(
    dag_id='scheduler_daily',
    description='DAG de teste para o scheduler cron no Airflow',
    scheduler_interval=dt.timedelta(minutes=5),  # executa a cada 5 minutos
    start_date=dt.datetime(2021, 1, 1),
)

print_date_task = BashOperator(
    task_id='print_date',
    bash_command=(
        "echo Iniciando a tarefa: &&"
        "date"
    ),
    dag=dag,
)

processing_task = BashOperator(
    task_id='processing_data',
    bash_command=(
        "echo Processando os dados... &&"
        "sleep 60"
    ),
    dag=dag
)

print_date_task >> processing_task
