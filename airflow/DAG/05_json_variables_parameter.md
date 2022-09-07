### Passando Variaveis por parametro

A forma ideal para passar as variáveis criadas por parâmetro.

Para criar uma variável:<br>
Via UI: Admin -> Variables

```python
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

def _extract(pessoa_nome, pessoa_idade):
    print(pessoa_nome)
    print(pessoa_idade)

with DAG( "_05_json_variables_parameter",
          description="Testes com variáveis. Olhar os comentários no início do código.", 
          start_date=datetime(2022,1,1), 
          schedule_interval="@daily",
          dagrun_timeout=timedelta(minutes=10),
          tags=["certificacao","variables", "astronomer"],
          catchup=False,
          max_active_runs=1
          ) as dag:

    extract = PythonOperator(
        task_id="extract",
        python_callable= _extract,
        op_args=["{{ var.json.pessoa.nome }}","{{ var.json.pessoa.idade }}"]
    )
```