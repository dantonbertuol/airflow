from airflow import DAG
import airflow.utils.dates
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.edgemodifier import Label

# Cria a DAG
dag = DAG(
    dag_id="edge_labels",
    description="DAG de exemplo para edge labels",
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
# Na tarefa notify_op a regra de acionamento é se nenhuma das tarefas anteriores falhou
# Se ela foi ignorada ou teve sucesso executa, esse operador de regra pode ser definido por Tarefa
# Se fosse no trigger_rule padrão, jamais seria executada pq na bifurcação uma será ignorada
notify_op = DummyOperator(task_id="notify_task", dag=dag, trigger_rule=TriggerRule.NONE_FAILED)

# Ordem de execução das tarefas
get_acuracy_op >> check_acuracy_op
check_acuracy_op >> Label("ACC >= 90%") >> deploy_op >> notify_op
check_acuracy_op >> Label("ACC < 90%") >> retrain_op >> notify_op
