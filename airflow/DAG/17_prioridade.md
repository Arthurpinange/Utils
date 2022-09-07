### definir prioridade entre task
ou seja quem executa primeiro

```python

    extract = PythonOperator(
        task_id="extract",
        python_callable= _extract
        priority=1 # quanto menor maior a prioridade, se a tesk estiver no mesmo nivel a que tem menor prioridade executa primeiro
    )
```

