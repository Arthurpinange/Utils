### Regas de gatilho
isso significa que um dag run vai escolher qual caminho seguir do fluxo de acordo com o resultado da tesk, como por exemplo seguir uma caminho se a task falhar.

por padrão a triger default é all_sucess, ou seja, a deg run avança para a proxima task se a task atual executar com sucesso, mas se quisermos seguir para uma task especifica se a task atual falhar ?

resolvermos isso com o parametro da task *trigger_rule*

```python
my_task = PythonOperator(
task_id='my_task',
trigger_rule='all_failed' # o default é all_success
)
```

existem outras opções conforme exibido abaixo:

### all_success
Este é bem direto, e você já viu, sua tarefa é acionada quando todas as tarefas upstream (pais) são bem-sucedidas.
![](/img/all_sucess.webp)
<br>
Uma ressalva, porém, se um dos pais for ignorado, a tarefa será ignorada, assim como mostrado abaixo:
![](/img/trigger_rules_all_success_skipped.webp)
#
### all_failed
Muito claro, sua tarefa é acionada se todas as tarefas pai falharem.
![](/img/trigger_rules_all_failed.webp)
<br>
Pode ser realmente útil se você quiser fazer alguma limpeza ou algo mais complexo que não possa colocar em um retorno de chamada. Você pode definir literalmente um caminho de tarefas a serem executadas se algumas tarefas falharem.

Como com all_success, se a Tarefa B for ignorada, a Tarefa C também será ignorada.
#
### all_done
Você só quer acionar sua tarefa quando todas as tarefas upstream (pais) terminarem com sua execução, independentemente do estado.
![](/img/trigger_rules_all_done.webp)
<br>
Esta regra de gatilho pode ser útil se houver uma tarefa que você sempre deseja executar, independentemente dos estados da tarefa upstream
#
### one_failed
Assim que uma das tarefas upstream falhar, sua tarefa será acionada.
![](/img/trigger_rules_one_failed.webp)
<br>
Pode ser útil se você tiver algumas tarefas de longa duração e quiser fazer algo assim que uma falhar.
#
### one_sucess
Como com one_failed, mas o oposto. Assim que uma das tarefas upstream for bem-sucedida, sua tarefa será acionada.
![](/img/trigger_rules_one_success.webp)
<br>
#
### none_failed
Sua tarefa é acionada se todas as tarefas upstream tiverem êxito ou forem ignoradas .

Útil apenas se você quiser lidar com o status ignorado.
![](/img/trigger_rules_none_failed-1.webp)
<br>
#
### none_failed_min_one_success
Antes conhecido como “none_failed_or_skipped” (antes do Airflow 2.2), com essa regra de gatilho, sua tarefa é acionada se todas as tarefas upstream não tiverem falhado e pelo menos uma tiver sido bem-sucedida.

Esta é a regra que você deve definir para lidar com a armadilha do BranchPythonOperator 😎
![](/img/trigger_rules_none_failed_min_one_success-1.webp)
#
### none_skipped
Com essa regra de gatilho simples, sua tarefa é acionada se nenhuma tarefa upstream for ignorada. Se todos eles são bem sucedidos ou falharam.