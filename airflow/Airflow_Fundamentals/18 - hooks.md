## Hooks

Os ganchos são um dos blocos de construção fundamentais do Airflow. Em alto nível, um gancho é uma abstração de uma API específica que permite que o Airflow interaja com um sistema externo. Os ganchos são incorporados a muitos operadores, mas também podem ser usados ​​diretamente no código DAG.

Hooks envolvem APIs e fornecem métodos para interagir com diferentes sistemas externos. Como os ganchos padronizam a maneira como você pode interagir com sistemas externos, usá-los torna seu código DAG mais limpo, mais fácil de ler e menos propenso a erros.

 - Os ganchos devem sempre ser usados ​​na interação manual da API para conectar-se a sistemas externos.
 - Se você escrever um operador personalizado para interagir com um sistema externo, ele deverá usar um gancho para fazer isso.

## Tipos de Ganchos 

- airflow.hooks.S3_hook
- airflow.hooks.base
- airflow.hooks.base_hook
- airflow.hooks.dbapi
- airflow.hooks.dbapi_hook
- airflow.hooks.docker_hook
- airflow.hooks.druid_hook
- airflow.hooks.filesystem
- airflow.hooks.hdfs_hook
- airflow.hooks.hive_hooks
- airflow.hooks.http_hook
- airflow.hooks.jdbc_hook
- airflow.hooks.mssql_hook
- airflow.hooks.mysql_hook
- airflow.hooks.oracle_hook
- airflow.hooks.pig_hook
- airflow.hooks.postgres_hook
- airflow.hooks.presto_hook
- airflow.hooks.samba_hook
- airflow.hooks.slack_hook
- airflow.hooks.sqlite_hook
- airflow.hooks.subprocess
- airflow.hooks.webhdfs_hook
- airflow.hooks.zendesk_hook

