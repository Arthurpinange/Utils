# Git
## Comandos basicos do Git

###  Instalação
#### executar no terminal do linux
``` sudo apt install git-all ```
### configurando a ssh 
criar o ssh
```
ssh-keygen -t ed25519 -C "your_email@example.com"
``` 
adiciona o ssh na maquina
```
ssh-add ~/.ssh/id_ed25519
```
teste o ssh 
 ```
ssh -T git@github.com
```

### inicialização
``` git init ```
para testar você pode executar:
``` git --version ```
### Clonar um repositório
``` git clone < url do repo  ou ssh do repo> ```
###  buscar alterações no repo remoto
executar sempre antes de realizar uma alteração ou criar uma nova branch
``` git pull ```
### criar uma branch
``` git checkout -b <nome_da_branch> ```
####  comando para verificar branch existentes e em qual esta
``` git branch ```
