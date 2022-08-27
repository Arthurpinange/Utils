Docker rmi
O comando docker rmi remove as imagens por ID.

Para remover a imagem, primeiro você precisa listar todas as imagens para obter os IDs de imagem, nomes de imagem e outros detalhes. Faça isso executando o comando docker images -a ou docker images.

Depois disso, certifique-se de qual imagem deseja remover, para depois fazer isso executando o comando docker rmi <id-da-imagem>. Em seguida, confirme que a imagem foi removida listando todas as imagens e verificando.

Remover várias imagens
Há uma forma de remover mais de uma imagem por vez quando você quiser remover diversas imagens. Para fazer isso, obtenha os IDs das imagens listando todas elas e execute o comando a seguir.

```bash
docker rmi <id-da-imagem> <id-da-imagem> ...
```
Escreva os IDs das imagens no comando acima com espaços entre eles.

Remover todas as imagens ao mesmo tempo
Para remover todas as imagens, há um comando que faz isso: 
```bash 
docker rmi $(docker images -q) 
```

No comando acima, há, de fato, dois comandos. O primeiro, executa em $(), que é a sintaxe do shell, e retorna seja quais forem os resultados executados naquela sintaxe. Enquanto isso, -q- é a opção usada para retornar os IDs exclusivos. $() retorna os resultados dos IDs de imagens e docker rmi remove todas elas.

Para obter mais informações:
Documentação da Docker CLI: rmi (em inglês)
Docker rm
docker rm remove contêineres por nome ou por ID.

Quando você tem contêineres do Docker em execução, é preciso pará-los primeiro antes de exclui-los.

Parar todos os contêineres em execução: docker stop $(docker ps -a -q)
Excluir todos os contêineres parados: docker rm $(docker ps -a -q)
Remover diversos contêineres
Você pode parar e excluir diversos contêineres passando para os comandos uma lista dos contêineres que deseja remover. A sintaxe do shell $() retorna os resultados do que tiver sido executado dentro dos parênteses. Desse modo, você pode criar sua lista de contêineres dentro dela para passar a informação para os comandos stop e rm.

Aqui, você verá um resumo de docker ps -a -q
docker ps lista contêineres
-a opção para listar todos os contêineres, mesmo os parados. Sem isso, o padrão é listar apenas os contêineres em execução
-q opção silenciosa para fornecer apenas os IDs numéricos dos contêineres, em vez de uma tabela inteira de informações sobre os contêineres