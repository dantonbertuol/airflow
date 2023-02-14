from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
import random

args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'description': 'DAG de teste para exemplificar o conceiuto de retries.',
}


def coleta_dados_web():
    n = random.random()
    print("Número gerado: ", n)
    if n > 0.3:
        raise Exception("Erro na coleta de dados da web.")
    print("Coleta OK!")


def processa_dados():
    print("Processando os dados...")


with DAG(dag_id="retries_example",
         default_args=args,
         schedule_interval=None
         ) as dag:

    coleta_dados_web_task = PythonOperator(
        task_id='coleta_dados_web',
        python_callable=coleta_dados_web,
        retries=10,
        retry_delay=timedelta(seconds=1)
    )

    processa_dados_task = PythonOperator(
        task_id='processa_dados',
        python_callable=processa_dados,
    )

    coleta_dados_web_task >> processa_dados_task
