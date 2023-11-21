# pucminas-projeto3
Extensão - Desenvolvimento de Projeto de Business Intelligence - 2023/02

### Desenho da arquitetura de dados
<img src="doc/architecture.png">

<br>

### Google Cloud
O projeto faz uso do Google Cloud, portanto as interações - extração e ingestão de dados - necessitam do credenciamento com a Google Cloud, tal credenciamento está sendo feito através de um arquivo chamado google_credential.json, que por motivos de segurança não está sendo exposto neste repositório do GitHub.

Para mais informações, sobre credenciamento da Google, acesse o link: [Google Credenciamento](https://developers.google.com/workspace/guides/create-credentials?hl=pt-br)

### Comandos úteis
1. Criação de um ambiente virtual para o python, necessário ter venv instalado
    ~~~sh
    python3 -m venv venv
    ~~~
2. Inicialização do ambiente virtual
    ~~~sh
    source venv/bin/activate
    ~~~
3. Instalação dos módulos necessários para rodar o projeto
    ~~~sh
    pip install -r requirements.txt
    ~~~
4. Criação de um módulo para ajuste dos imports das libs, necessário estar na raiz do projeto
    ~~~sh
    pip install -e .
    ~~~
