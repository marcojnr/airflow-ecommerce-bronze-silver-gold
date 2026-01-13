# airflow-ecommerce-bronze-silver-gold


### Resumo
Projeto para tratar dados com modelo de camadas bronze, silver e gold, (escrever mais..)

### Estrutura do repositório
- [data/......csv]— dataset de exemplo Olist Data.
- [src/t,,,.py](src/,,,,.py) — script que injeta eventos (simulação de produtor).
- [docs/diagrama-case.drawio](docs/diagrama-case.drawio) — diagrama arquitetural.
- [requirements.txt](requirements.txt) — dependências Python.
- [.gitignore](.gitignore)
  
### Pré-requisitos
- Python 3.8+
- Criar e ativar um ambiente virtual (recomendado)
- Instalar dependências:
```sh
python -m venv .venv
source .venv/bin/activate  # macOS / Linux
.venv\Scripts\activate     # Windows
pip install -r requirements.txt
```

### Execução