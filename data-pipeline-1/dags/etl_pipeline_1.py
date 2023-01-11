from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.email import EmailOperator
from airflow.operators.python import PythonOperator
import pandas as pd
import sqlalchemy

path_temp_csv = "/tmp/dataset.csv"
email_failed = "dantonjb@gmail.com"

dag = DAG(
    dag_id='elt-pipeline1',
    description='Pipeline para o processo de ETL dos ambientes de produção oltp ao olap.',
    start_date=days_ago(2),
    schedule_interval=None,
)


def _extract():
    # conectando a base de dados do oltp
    engine_mysql_oltp = sqlalchemy.create_engine('mysql+pymysql://root:airflow@172.17.0.2:3306/employees')

    # selecionando os dados
    dataset_df = pd.read_sql_query(r"""
                                    SELECT
                                        emp.emp_no ,
                                        emp.first_name ,
                                        emp. last_name ,
                                        sal.salary ,
                                        titles.title
                                    FROM
                                        employees emp
                                    INNER JOIN (
                                        SELECT
                                            emp_no,
                                            MAX(salary) as salary
                                        FROM
                                            salaries
                                        GROUP BY
                                            emp_no) sal ON
                                        sal.emp_no = emp.emp_no
                                    INNER JOIN titles ON
                                        titles.emp_no = emp.emp_no
                                    LIMIT 100""",
                                   engine_mysql_oltp)
    dataset_df.to_csv(path_temp_csv, index=False)


def _transform():
    dataset_df = pd.read_csv(path_temp_csv)

    dataset_df["name"] = dataset_df['first_name'] + " " + dataset_df['last_name']

    dataset_df.drop(["emp_no",
                     "first_name",
                     "last_name"
                     ],
                    axis=1,
                    inplace=True)

    # persistindo o dataset no arquivo temporario
    dataset_df.to_csv(path_temp_csv, index=False)


def _load():
    # conectando com o banco de dados postgresql
    engine_postgresql_olap = sqlalchemy.create_engine(
        'postgresql+psycopg2://postgres:airflow@172.17.0.3:5432/employees')

    # lendo os dados a partir do csv
    dataset_df = pd.read_csv(path_temp_csv)

    # carregando os dados no banco de dados
    dataset_df.to_sql("employees_dataset", engine_postgresql_olap, if_exists='replace', index=False)


extract_task = PythonOperator(
    task_id="Extract_Dataset",
    python_callable=_extract,
    dag=dag
)

transform_dast = PythonOperator(
    task_id="Transform_Dataset",
    python_callable=_transform,
    dag=dag
)

load_task = PythonOperator(
    task_id="Load_Dataset",
    email_on_failure=True,
    email=email_failed,
    python_callable=_load,
    dag=dag
)

clean_task = BashOperator(
    task_id="Clean",
    bash_command="scripts/clean.sh",
    dag=dag
)

email_task = EmailOperator(
    task_id="Notify",
    to=email_failed,
    subject="Pipeline Finalizado",
    html_content='<p> O Pipeline para atualização de dados entre '
    'os ambientes OLTP e OLAP foi finalizado com sucesso. </p>',
    dag=dag
)

extract_task >> transform_dast >> load_task >> clean_task >> email_task
