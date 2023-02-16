from airflow import DAG
from airflow.providers.mysql.operators.mysql import MySqlOperator
import datetime as dt

with DAG(
    dag_id="template_sql_query",
    description="Template SQL query",
    start_date=dt.datetime(2021, 1, 1),
    schedule_interval="@daily"
) as dag:

    drop_table_if_exist = MySqlOperator(
        task_id="drop_table_if_exist",
        mysql_conn_id='mysql_conn_oltp',
        sql="DROP TABLE IF EXISTS sales_temp;",
    )

    create_table_mysql_task = MySqlOperator(
        task_id="create_table_mysql",
        mysql_conn_id='mysql_conn_oltp',
        sql="""
                CREATE TABLE sales_temp AS
                SELECT * FROM sales
                WHERE data BETWEEN "{{ execution_date.strftime('%Y-%m-%d') }}"
                AND "{{ execution_date.strftime('%Y-%m-%d') }}";
             """
    )

drop_table_if_exist >> create_table_mysql_task
