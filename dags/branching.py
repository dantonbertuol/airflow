from airflow import DAG
import airflow.utils.dates
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import BranchPythonOperator

# Cria a DAG
dag = DAG(
    dag_id="branching-teste",
    description="DAG de exemplo para o controle de Branching",
    start_date=airflow.utils.dates.days_ago(14),
    schedule_interval=None,
)


def check_acuracy(ti):
    # Função que verifica a acurácia do modelo
    acuracy_value = int(ti.xcom_pull(key="model_acuracy"))  # xcom_pull retorna o valor da chave "model_acuracy"
    if acuracy_value >= 90:
        return "deploy_task"  # nome da tarefa que vai seguir no fluxo
    else:
        return "retrain_task"  # nome da tarefa que vai seguir no fluxo


# Bash Operator que insere o valor da acurácia no XCom
get_acuracy_op = BashOperator(
    task_id="get_acuracy_task",
    bash_command='echo "{{ ti.xcom_push(key="model_acuracy", value=80) }}"',
    dag=dag,
)

# Python Operator que verifica a acurácia do modelo e executa a tarefa correspondente
check_acuracy_op = BranchPythonOperator(
    task_id="check_acuracy_task",
    python_callable=check_acuracy,
    dag=dag,
)

deploy_op = DummyOperator(task_id="deploy_task", dag=dag)
retrain_op = DummyOperator(task_id="retrain_task", dag=dag)

# Ordem de execução das tarefas sendo o último uma bifurcação
get_acuracy_op >> check_acuracy_op >> [deploy_op, retrain_op]
