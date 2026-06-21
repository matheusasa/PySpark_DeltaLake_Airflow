# Observabilidade de Tempo de Ingestao — Pipeline E-commerce

## Resumo

Instrumentacao completa do pipeline Bronze/Silver/Gold com monitoring de tempo de ingestao por tabela e por fase (leitura/transformacao/escrita). Usa Prometheus + Grafana para dashboards, tabela Delta de auditoria para consultas historicas, e logging estruturado JSON.

## Arquitetura

### Novos Containers (docker-compose.yml)

| Container | Imagem | Porta | Funcao |
|-----------|--------|-------|--------|
| `prometheus` | `prom/prometheus` | 9090 | Scrapes Pushgateway + Airflow exporter |
| `pushgateway` | `prom/pushgateway` | 9091 | Recebe push dos scripts batch |
| `grafana` | `grafana/grafana` | 3000 | Dashboard de observabilidade |

### Fluxo de Metricas

```
run_bronze.py/silver/gold → Pushgateway → Prometheus scrape → Grafana dashboard
Airflow tasks → Airflow Prometheus exporter (porta 9092) → Prometheus scrape → Grafana
```

## Modulo de Instrumentacao (`tools/observabilidade.py`)

### Context Manager Principal

```python
@contextmanager
def medir_fase(camada: str, tabela: str, fase: str):
    """Mede tempo de uma fase, loga JSON, push para Pushgateway."""
```

Comportamento por fase:
1. Registra timestamp de inicio
2. Executa o bloco de codigo (yield)
3. Em sucesso: calcula duracao, loga JSON, push metrica para Pushgateway, grava na tabela pipeline_audit
4. Em erro: loga JSON com status=erro + traceback, push metrica com status=erro

### Log JSON (stdout)

Formato de cada linha:
```json
{
  "timestamp": "2026-06-21T14:30:00.123Z",
  "level": "INFO",
  "camada": "bronze",
  "tabela": "vendas",
  "fase": "leitura",
  "duracao_ms": 12500,
  "row_count": 1000000,
  "status": "sucesso"
}
```

### Metricas Prometheus

| Metrica | Labels | Tipo | Descricao |
|---------|--------|------|-----------|
| `pipeline_fase_duration_seconds` | camada, tabela, fase, status | Gauge | Duracao de cada fase |
| `pipeline_fase_row_count` | camada, tabela, fase | Gauge | Linhas processadas |
| `pipeline_camada_duration_seconds` | camada, status | Gauge | Duracao total da camada |
| `pipeline_run_total` | camada, status | Counter | Numero de runs |

Push via `prometheus_client` para `pushgateway:9091` no job `pipeline_ecommerce`.

### Novos Arquivos

| Arquivo | Funcao |
|---------|--------|
| `tools/observabilidade.py` | Context managers de timing + JSON logging + push Prometheus + gravar audit |
| `config/prometheus.yml` | Scrape config: Pushgateway (9091) + Airflow exporter (9092) |
| `grafana/provisioning/datasources/prometheus.yml` | Datasource Prometheus auto-configurada |
| `grafana/provisioning/dashboards/dashboard.yml` | Dashboard provisionado automaticamente |
| `grafana/dashboards/pipeline-observabilidade.json` | JSON do dashboard Grafana |

### Nova Dependencia

`prometheus-client` adicionado ao container Spark (via Dockerfile ou `pip install` no docker-compose).

## Modificacao dos Scripts

### Padrao de instrumentacao (run_bronze.py, run_silver.py, run_gold.py)

```python
from observabilidade import medir_fase, RunContext

# No inicio do script:
run_ctx = RunContext(camada="bronze", run_id=str(uuid4()))

# Para cada tabela:
with medir_fase("bronze", tabela, "leitura", run_ctx=run_ctx):
    df = spark.read.csv(caminho, header=True, inferSchema=True)
    count = df.count()

with medir_fase("bronze", tabela, "transformacao", run_ctx=run_ctx, row_count=count):
    # transformacoes se houver

with medir_fase("bronze", tabela, "escrita", run_ctx=run_ctx, row_count=count):
    df.write.format("delta").mode("overwrite").save(path)

# No final do script:
run_ctx.finalizar()
# Gera arquivo JSON de resumo em /tmp/{camada}_metrics.json para XCom
```

### Tratamento de Erro

- try/except ao redor de cada tabela: falha em uma tabela nao aborta as demais
- Log com level ERROR e traceback completo
- Push metrica com status="erro"
- Grava registro na pipeline_audit com status erro
- Exit code 1 se qualquer tabela falhou

## Tabela Delta de Auditoria (`warehouse/pipeline_audit`)

Append-only, nunca overwrite.

| Coluna | Tipo | Descricao |
|--------|------|-----------|
| `run_id` | string | UUID do run |
| `timestamp` | timestamp | Inicio da fase |
| `camada` | string | bronze/silver/gold |
| `tabela` | string | Nome da tabela |
| `fase` | string | leitura/transformacao/escrita |
| `duracao_ms` | long | Duracao em milissegundos |
| `row_count` | long | Linhas processadas |
| `status` | string | sucesso/erro |
| `erro_mensagem` | string | Null em caso de sucesso |

## Tabelas Gold de Observabilidade

### `gold/duracao_fase_tabela`
- Granularidade: camada + tabela + fase
- Metricas: duracao_media_ms, duracao_min_ms, duracao_max_ms, duracao_p95_ms, qtd_runs
- Janela: ultimos 30 runs por grupo

### `gold/duracao_camada_total`
- Granularidade: camada + run_id
- Metricas: duracao_total_ms, variacao_vs_run_anterior_pct, qtd_tabelas_sucesso, qtd_tabelas_erro

### `gold/volume_dados_por_tabela`
- Granularidade: camada + tabela + run_id
- Metricas: row_count, variacao_vs_run_anterior_pct

Geradas pelo script `tools/run_gold.py` apos as agregacoes de negocio existentes.

## Airflow — DAG Modificado

### SLAs por Task

| Task | SLA |
|------|-----|
| `run_bronze` | 10 minutos |
| `run_silver` | 15 minutos |
| `run_gold` | 10 minutos |

SLA global do DAG: 30 minutos.

### on_failure_callback

Funcao que loga em JSON structured format com: dag_id, task_id, execution_date, exception. Log aparece no Airflow UI.

### XCom com Metricas

- Scripts geram `/tmp/{camada}_metrics.json` com resumo do run
- DAG le o arquivo apos cada BashOperator e faz `xcom_push(key="metrics_{camada}")`
- Tarefas downstream podem consultar metricas da camada anterior

### Prometheus Exporter do Airflow

- `airflow-provider-prometheus` adicionado a `airflow/requirements.txt`
- Porta do exporter: 9092
- Prometheus faz scrape dessa porta

## Grafana Dashboard

### Painel 1: Duracao por Camada (time series)
- `pipeline_camada_duration_seconds` por camada ao longo do tempo
- Uma serie por camada

### Painel 2: Duracao por Tabela e Fase (heatmap)
- `pipeline_fase_duration_seconds` agrupado por camada/tabela/fase

### Painel 3: Volume de Dados (barras empilhadas)
- `pipeline_fase_row_count` por tabela por run

### Painel 4: Status das Ultimas Runs (stat)
- Ultimas runs com status e duracao total
- Vermelho para falhas

### Painel 5: SLA Breaches (gauge)
- Contagem de SLA breaches por task nas ultimas 24h

### Painel 6: Tendencia por Fase (time series)
- Evolucao da duracao leitura/transformacao/escrita de uma tabela especifica

## Arquivos Modificados

| Arquivo | Tipo de Mudanca |
|---------|----------------|
| `tools/run_bronze.py` | Adicionar instrumentacao com `medir_fase` + tratamento de erro |
| `tools/run_silver.py` | Adicionar instrumentacao com `medir_fase` + tratamento de erro |
| `tools/run_gold.py` | Adicionar instrumentacao + gerar tabelas Gold de observabilidade |
| `airflow/dags/gerar_dados_dag.py` | SLAs, callbacks, XCom, metricas |
| `airflow/requirements.txt` | Adicionar `airflow-provider-prometheus` |
| `docker-compose.yml` | Adicionar 3 containers (prometheus, pushgateway, grafana) + rede |

## Arquivos Novos

| Arquivo | Funcao |
|---------|--------|
| `tools/observabilidade.py` | Modulo de instrumentacao |
| `config/prometheus.yml` | Config Prometheus |
| `grafana/provisioning/datasources/prometheus.yml` | Datasource Grafana |
| `grafana/provisioning/dashboards/dashboard.yml` | Provisionamento de dashboards |
| `grafana/dashboards/pipeline-observabilidade.json` | Dashboard JSON |
