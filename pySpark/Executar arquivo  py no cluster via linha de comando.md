Local mode:



O modo local executa o driver e os executors em uma única máquina. Neste modo, as partições são processadas por várias threads em paralelo. O número de threads pode ser controlado pelo usuário durante o envio do job. Este modo é útil na fase de aprendizado, mas não é recomendado para aplicativos de produção, já que você usa apenas uma máquina para processar os dados. O seguinte mostra como você pode enviar um job no modo local com spark-submit:

bom para usar ao testar alguma coisa.

Comando: $ spark-submit --master local example.py



Client mode:



No client mode, o driver process é executado no nó client no qual o job foi enviado. O nó client fornece recursos, como memória, CPU e espaço em disco para o driver program, mas os executors são executados nos nós do cluster e são mantidos pelo gerenciador do cluster, como YARN.



Comando:



$ spark-submit \

    --master yarn

    --deploy-mode client

    --num-executors 3

    --executor-memory 2g \

    --total-executor-cores 1 \

    example.py




Cluster Mode:

modelo usado em prod mais usado


O modo de cluster é semelhante ao client mode, exceto que o driver process é executado em um dos workers do cluster e o gerenciador do cluster é responsável pelo driver e executor processes . Isso oferece a vantagem de executar vários aplicativos ao mesmo tempo, porque o gerenciador de cluster distribuirá a carga do driver pelo cluster. Este modo é o modo mais comum e recomendado para executar os aplicativos Spark. Nesse modo, os logs podem ser coletados do gerenciador de cluster ou você pode implementar uma solução de log central para reunir os logs do aplicativo.



Comando:



$ spark-submit \

    --master yarn

    --deploy-mode cluster

    --num-executors 3

    --executor-memory 2g \

    --total-executor-cores 1 \

    example.py