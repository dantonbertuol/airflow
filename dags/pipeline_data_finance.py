import datetime as dt
import requests
import pandas as pd
from airflow import DAG
from airflow.operator.python import PythonOperator

dag = DAG(
    dag_id="pipeline_data_finance",
    description="DAG de teste para coletar dados do mercado financeiro por datas dinamicamente atribuidas.",
    schedule_interval="@daily",
    start_date=dt.datetime(2023, 2, 14),
    end_date=dt.datetime(2023, 2, 19),
)


def get_data_stocks(**context):
    start_date = context["templates_dict"]["start_date"]
    end_date = context["templates_dict"]["end_date"]
    ativo = "AAPL"
    print("Start date: {}".format(start_date))

    datafile = "/tmp/dataset-{}.csv".format(start_date)
    api_key = "YOUR_API_KEY"

    url = 'https://api.polygon.io/v2/aggs/ticker/{}/range/1/day/{}/{}?apiKey={}'.format(
        ativo, start_date, end_date, api_key)

    r = requests.get(url)
    data = r.json()

    if data["count"] > 0:
        # Transforma o dicionário em um dataframe
        df = pd.DataFrame(data["results"])
        df["date"] = start_date

        # Exporta o dataframe para um arquivo CSV
        df.to_csv(datafile, index=False)


get_data_stocks_task = PythonOperator(
    task_id="get_data_stocks",
    python_callable=get_data_stocks,
    templates_dict={  # o templates_dict vai no contexto da função, e pode ser acessado com context["templates_dict"]
        "start_date": "{{ ds }}",
        "end_date": "{{ ds }}"
    },
    dag=dag,
)

get_data_stocks_task
