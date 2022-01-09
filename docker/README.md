# Instalação do docker 
Primeiro, atualize sua lista de pacotes existente:

```bash
sudo apt update
```

Em seguida, instale alguns pacotes de pré-requisitos que permitem o `apt`uso de pacotes por HTTPS:

```bash
sudo apt install apt-transport-https ca-certificates curl software-properties-common
```
Em seguida, adicione a chave GPG para o repositório oficial do Docker ao seu sistema:

```bash
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo apt-key add -
```
Adicione o repositório Docker às fontes APT:

```bash
sudo add-apt-repository "deb [arch=amd64] https://download.docker.com/linux/ubuntu focal stable"
```
Isso também atualizará nosso banco de dados de pacotes com os pacotes Docker do repo recém-adicionado.

Certifique-se de que está prestes a instalar a partir do repositório Docker em vez do repositório Ubuntu padrão:

```bash
apt-cache policy docker-ce
```
Finalmente, instale o Docker:

```bash
sudo apt install docker-ce
```

O Docker agora deve estar instalado, o daemon iniciado e o processo habilitado para iniciar na inicialização. Verifique se ele está funcionando:

```bash
sudo systemctl status docker

```
Normalmente, os usuários têm várias imagens no próprio sistema. Podemos listar todas elas usando:

```bash
sudo docker images

```
A listagem vai ser bem parecida com a lista que você recebe quando você faz uma pesquisa (query).

Depois disso, você pode executar a imagem usando o comando pull e a ID da imagem.

```bash
sudo docker run [image]

```
Existem opções que estendem a funcionalidade do comando. Por exemplo, a opção -i faz com que a execução da imagem seja interativa. Já a opção -d faz com que ela aconteça em segundo plano.

Assim que estiver executando uma imagem, podemos terminar a execução usando a combinação de teclas CTRL+D.