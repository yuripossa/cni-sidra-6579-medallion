# CNI – Avaliação Técnica (Tabela SIDRA 6579) | Arquitetura Medallion (Bronze/Silver/Gold)

Este repositório implementa um pipeline de engenharia de dados com a arquitetura **Medallion**:
- **Bronze**: ingestão bruta via API do IBGE/SIDRA (JSON com metadados de ingestão)
- **Silver**: normalização e tipagem básica (Parquet)
- **Gold**: dados prontos para consumo analítico (agregações + amostras versionadas)

## Fonte de dados
- API IBGE/SIDRA – Tabela **6579**
- Endpoint: `https://apisidra.ibge.gov.br/values/t/6579/n3/all/p/all/v/all`

<img width="1902" height="986" alt="image" src="https://github.com/yuripossa/cni-sidra-6579-medallion/evidencias/databricks2.png" />
## Estrutura do projeto
src/
ingest/
01_ingest_sidra.py # Bronze: baixa e salva JSON bruto
transform/
02_bronze_to_silver.py # Silver: normaliza e salva Parquet
consume/
03_silver_to_gold.py # Gold: agregações para Analytics

docs/
gold_uf_sample.csv # amostra (versionada) do gold por UF
gold_brasil_sample.csv # amostra (versionada) do total Brasil

data/ (não versionado)
bronze/ # JSON bruto
silver/ # Parquet normalizado
gold/ # Parquet analítico

## Como executar (local / WSL)
> O repositório não versiona `.venv/` nem `data/` por boas práticas.

1) Criar e ativar virtualenv:
```bash
python3 -m venv .venv
source .venv/bin/activate
```
Instalar dependências:

```bash
python -m pip install -r requirements.txt

```
Executar camadas:

python src/ingest/01_ingest_sidra.py
python src/transform/02_bronze_to_silver.py
python src/consume/03_silver_to_gold.py

Saídas geradas

Bronze: data/bronze/sidra_6579_<batch_id>.json

Silver: data/silver/sidra_6579_silver_<batch_id>.parquet

Gold:

data/gold/sidra_6579_gold_uf_<batch_id>.parquet (por UF e variável)

data/gold/sidra_6579_gold_brasil_<batch_id>.parquet (total Brasil por variável)

Evidência (versionada):

docs/gold_uf_sample.csv

docs/gold_brasil_sample.csv

Decisões técnicas

Bronze em JSON para manter fidelidade ao payload original + metadados de ingestão (batch_id, timestamp, URL).

Silver em Parquet para padronizar e otimizar leitura/consumo (formato colunar).

Gold com agregações para consumo em Data & Analytics (camada pronta para BI e análises).

Como seria em produção na Azure (ADF + Databricks)

ADF: orquestração (schedule, parâmetros, retries, logs, alertas, integração com Key Vault)

Databricks: processamento (notebooks/jobs) e escrita das camadas Bronze/Silver/Gold (idealmente em Delta Lake)

Evidências no Databricks:
<img width="1902" height="986" alt="image" src="https://github.com/yuripossa/cni-sidra-6579-medallion/evidencias/databricks1.png" />
<br>
<img width="1902" height="986" alt="image" src="https://github.com/yuripossa/cni-sidra-6579-medallion/evidencias/notebooks_python.png" />
<br>
<img width="1902" height="986" alt="image" src="https://github.com/yuripossa/cni-sidra-6579-medallion/evidencias/armazenamento_dados.png" />

DevOps: versionamento do código, revisão (PR), CI/CD para promover mudanças entre DEV/HML/PRD.


