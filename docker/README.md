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
