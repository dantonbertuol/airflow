from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

dag = DAG(
    dag_id='scheduler_daily',
    description='DAG de teste para o scheduler cron no Airflow',
    scheduler_interval='15 23 * * *',  # executa todo dia às 23:15
    start_date=datetime(2021, 1, 1),
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
