### O Operador BranchPython
O BranchPythonOperator permite seguir um caminho específico de acordo com uma condição . Essa condição é avaliada em uma função que pode ser chamada em python. Assim como o PythonOperator, o BranchPythonOperator executa uma função Python retornando o ID da tarefa da próxima tarefa a ser executada . Veja o exemplo abaixo. Você consegue adivinhar qual tarefa é executada em seguida? “É preciso” ou “É impreciso”?
![](/img/Branching.jpg)
#
### from airflow.operators.python import BranchPythonOperator
#
```python
from airflow import DAG
from airflow.operators.python import BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime
default_args = {
'start_date': datetime(2020, 1, 1)
}

def _choose_best_model():
    accuracy = 6
    if accuracy > 5:
        return 'accurate'
    return 'inaccurate'

with DAG('branching', schedule_interval='@daily', default_args=default_args, catchup=False) as dag:
    choose_best_model = BranchPythonOperator(
        task_id='choose_best_model',
        python_callable=_choose_best_model
        )
    accurate = DummyOperator(
        task_id='accurate'
        )
    inaccurate = DummyOperator(
    task_id='inaccurate'
        )
choose_best_model >> [accurate, inaccurate]
```
