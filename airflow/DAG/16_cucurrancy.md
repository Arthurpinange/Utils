### Definir numeros de task que rodam ao mesmo tempo 
### cucurrancy

```python
from airflow import DAG
from datetime import datetime, timedelta

with DAG( "_01_my_dag", # dag_id: Deve ser único entre todas as dags!
          description="dag in charge of processing whatever it has to do", 
          # Cuidado ao rodar a primeira vez com um itervalo muito grande, a primeira execução irá disparar todos os agendamentos ao mesmo tempo!
          start_date=datetime(2022,1,1), 
          schedule_interval="@daily",
          dagrun_timeout=timedelta(minutes=10), # Para encerrar caso passe o tepo de execução. Se não for interrompido, dois processos serão executados ao mesmo tempo.
          tags=["certificacao","tutorial", "astronomer"],
          max_active_runs=1,
          catchup=False,
          cucurrancy=1 # nesse caso só uma task por vez vai executar
          ) as dag:
    None
```