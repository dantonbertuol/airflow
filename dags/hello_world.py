from airflow import DAG
import airflow.utils.dates
from airflow.operators.bash import BashOperator

# Instancia a DAG
dag = DAG(
    dag_id="hello_world",  # ID da Dag
    description="A primeira DAG de teste do airflow",  # Descrição da Dag
    start_date=airflow.utils.dates.days_ago(14),  # Inicia 14 dias atrás
    schedule_interval="@daily",  # Executa diariamente
)

# Instancia a tarefa BashOperator (executa um comando bash)
task_echo_message = BashOperator(
    task_id="echo_message",  # ID da tarefa
    bash_command="echo Hello World!",  # Comando bash
    dag=dag,  # Instância da DAG
)

# Executa a tarefa
task_echo_message
