
### Variavel com json no value

podemos guardar valores no formato de dicionario e recuperar

```python
# Para criar uma variável:
# Via UI: Admin -> Variables

# key    - value
# pessoa - {"nome":"My_Varieble", "idade":"40", "api_secret":"MySecret"}



from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

def _extract():
    pessoa = Variable.get("pessoa", deserialize_json=True)
    print(pessoa["nome"])
    print(pessoa["idade"])
    print(pessoa["api_secret"])

with DAG( "_04_json_variables",
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
        python_callable= _extract
    )
```