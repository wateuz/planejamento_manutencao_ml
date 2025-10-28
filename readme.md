<div align="center">
  <h1 align="center">
    PMML (Planejamento de Manuteção com Machine Learning)
    <br />
    <br />
    <!--a href="https://">
      <img src="https://slash-introducing.svg" alt="">
    </a-->
  </h1>
</div>

## Introduction

PMML é um projeto voltado para o desenvolvimento de modelos preditivos de falhas em sistemas industriais, utilizando dados históricos de manutenção e leituras de sensores dos equipamentos. O objetivo é antecipar falhas e otimizar o planejamento de manutenções, promovendo maior eficiência operacional no ambiente fabril.

### Paths and archives of project

- **data collection**
> O banco de dados <b style="color: green">dados_sensores.db</b> contém toda a parte dos dados estruturados e semi-estruturado.

- **archives**
> O <b style="color: blue">main.py</b> é o arquivo para aquisição dos dados.
> <b style="color: blue">standardize.py</b> é responsável por normalizar os dados armazenados no <b style="color: orange"> dados_sensores.db</b>.


## How to use
### Instalation
#### Comandos no cmd
```bash
git clone <link_deste_repositório>
# Criar virtual enviroment (para windows)
python -m venv venv
# Ativa a venv
.\venv\scripts\activate
# Instala as dependencias do projeto
pip install -r requirements.txt
```
No arquivo *standardize.py* ao final da execução o algoritmo executa:

```bash
155 line| df_imputed.to_csv("leituras_padronizadas_imputed.csv", index=True)
```
**OBS**: Pode ser alterada para outra forma de salvamento. Basta alterar ao final do arquivo o tipo de salvamento do dataframe *"df_imputed"*.