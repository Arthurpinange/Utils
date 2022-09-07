### Taskgroup é a forma ideal para agrupar as tasks.
# 

o código fica muito mais limpo e visualmente na UI, fica muito melhor vendo agrupado.

Nesta V1, o objetivo é limpar o código e mostrar o funcionamento.

```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.decorators import task, dag, task_group

from airflow.utils.task_group import TaskGroup

from datetime import datetime, timedelta
from typing import Dict


task_process  = BashOperator(task_id=f'task_process_a', bash_command='exit 0')
ask_store  = BashOperator(task_id=f'task_store_a', bash_command='exit 0')


default_args = {
    "start_date": datetime(2022,1,1)
}

@dag(
    description="DAG para realizar o processamento de dados",
    default_args=default_args, # Feito desta forma para compartilhar o mesmo valor de start_date para a dag como para a subdag
    schedule_interval="@daily",
    dagrun_timeout=timedelta(minutes=10),
    tags=["data_science", "partners"],
    catchup=False,
    max_active_runs=1
)
@task_group(group_id=f'path_a')
def path():
    task_process >> task_store

dag = path()
```