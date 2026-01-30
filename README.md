# CNI – Avaliação Técnica (Tabela SIDRA 6579) | Arquitetura Medallion (Bronze/Silver/Gold)

Este repositório implementa um pipeline de engenharia de dados com a arquitetura **Medallion**:
- **Bronze**: ingestão bruta via API do IBGE/SIDRA (JSON com metadados de ingestão)
- **Silver**: normalização e tipagem básica (Parquet)
- **Gold**: dados prontos para consumo analítico (agregações + amostras versionadas)

## Fonte de dados
- API IBGE/SIDRA – Tabela **6579**
- Endpoint: `https://apisidra.ibge.gov.br/values/t/6579/n3/all/p/all/v/all`


## Estrutura do projeto
<img width="600" alt="image" src="/evidencias/databricks2.png" />
src/ <br>
ingest/<br>
01_ingest_sidra.py # Bronze: baixa e salva JSON bruto<br>
transform/<br>
02_bronze_to_silver.py # Silver: normaliza e salva Parquet<br>
consume/<br>
03_silver_to_gold.py # Gold: agregações para Analytics<br>

docs/<br>
gold_uf_sample.csv # amostra (versionada) do gold por UF<br>
gold_brasil_sample.csv # amostra (versionada) do total Brasil<br>

data/ (não versionado)<br>
bronze/ # JSON bruto<br>
silver/ # Parquet normalizado<br>
gold/ # Parquet analítico<br>

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
```bash
python src/ingest/01_ingest_sidra.py
python src/transform/02_bronze_to_silver.py
python src/consume/03_silver_to_gold.py
```
## Saídas geradas

Bronze: data/bronze/sidra_6579_<batch_id>.json<br>

Silver: data/silver/sidra_6579_silver_<batch_id>.parquet<br><br>

Gold:<br>

data/gold/sidra_6579_gold_uf_<batch_id>.parquet (por UF e variável)<br>

data/gold/sidra_6579_gold_brasil_<batch_id>.parquet (total Brasil por variável)<br>

Evidência (versionada):<br><br>

docs/gold_uf_sample.csv<br>

docs/gold_brasil_sample.csv<br>

Decisões técnicas<br>

Bronze em JSON para manter fidelidade ao payload original + metadados de ingestão (batch_id, timestamp, URL).<br>

Silver em Parquet para padronizar e otimizar leitura/consumo (formato colunar).<br>

Gold com agregações para consumo em Data & Analytics (camada pronta para BI e análises).<br>

Como seria em produção na Azure (ADF + Databricks)<br>

ADF: orquestração (schedule, parâmetros, retries, logs, alertas, integração com Key Vault)<br>

Databricks: processamento (notebooks/jobs) e escrita das camadas Bronze/Silver/Gold (idealmente em Delta Lake)<br>

Evidências no Databricks:
<br> Estrutura no Databricks
<img width="1902" height="986" alt="image" src="/evidencias/databricks1.png" />
<br> Notebooks Python para realização das transformações
<img width="1902" height="986" alt="image" src="/evidencias/notebooks_python.png" />
<br> Dados armazenados de forma simples só para exemplificar, mas seria em banco de dados relacional ou não relacional
<img width="1902" height="986" alt="image" src="/evidencias/armazenamento_dados.png" />

DevOps: versionamento do código, revisão (PR), CI/CD para promover mudanças entre DEV/HML/PRD.

## Como seria em produção na Azure (ADF + Databricks)

Azure Data Factory (ADF): orquestração (schedule, parâmetros, retries, logs, alertas, integração com Key Vault).<br><br>

Databricks: processamento (notebooks/jobs) e escrita Bronze/Silver/Gold (idealmente em Delta Lake).<br><br>

DevOps: versionamento, revisão via PR e CI/CD para promover mudanças entre DEV/HML/PRD.<br><br>
