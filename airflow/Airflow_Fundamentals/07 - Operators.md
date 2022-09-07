## Operators

Um Operador é uma tarefa

        task1 = DummyOperator(
            task_id = 'task_1'
        )

Sempre utilizar um operador para cada tarefas, mesmo que seja do mesmo tipo.

        task1 = DummyOperator(
            task_id = 'task_1'
        )

        task2 = DummyOperator(
            task_id = 'task_2'
        )

Alguns parâmetros:

        task1 = DummyOperator(
            task_id = 'task_1',
            retry = 5,
            retry_delay=timedelta(minutes=5)
        )

E se eu precisar reaproveitar estes parâmetros para outras tarefas?
Usar o parâmetro *default_args* na DAG. 
(ver a dag [default_parameters.py](./dags/default_parameters.py))

## Operators customizados

É preciso importar o *BaseOperador* e criar uma classe que herda da classe importada, os metodos *__ init __* e *execute* são obrigatórios, o método *execute* será executado pelo DAG por padrão.

```
from airflow.models.baseoperator import BaseOperator

class HelloOperator(BaseOperator):
    def __init__(self, name: str, **kwargs) -> None:
        super().__init__(**kwargs)
        self.name = name

    def execute(self, context):
        message = f"Hello {self.name}"
        print(message)
        return message

```
Você pode usar modelos Jinja para parametrizar seu operador. O Airflow considera os nomes de campo presentes template_fields para modelagem ao renderizar o operador. Mais informações na documentação [Operatiors](https://airflow.apache.org/docs/apache-airflow/stable/howto/custom-operator.html)


```
class HelloOperator(BaseOperator):

    template_fields: Sequence[str] = ("name",)

    def __init__(self, name: str, **kwargs) -> None:
        super().__init__(**kwargs)
        self.name = name

    def execute(self, context):
        message = f"Hello from {self.name}"
        print(message)
        return message
```
Você pode usar o modelo da seguinte forma:

```
with dag:
    hello_task = HelloOperator(task_id="task_id_1", dag=dag, name="{{ task_instance.task_id }}")
```