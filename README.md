# Spark Delta Lake - Pipeline de Dados E-commerce

Pipeline de dados implementando a **arquitetura medallion** (Bronze -> Silver -> Gold) com Apache Spark e Delta Lake para processamento de dados de e-commerce brasileiro, com observabilidade via Prometheus e Grafana.

## Arquitetura

```
datasets/ (CSVs brutos com timestamp)
    |
    v
+---------------------------------------------+
|  BRONZE  — Ingestao de dados brutos          |
|  (schema original, sem transformacao)        |
|  8 tabelas: clientes, produtos, vendas,      |
|  fretes, pagamentos, cupons, avaliacoes,     |
|  fornecedores                                |
+---------------------+-----------------------+
                      |
                      v
+---------------------------------------------+
|  SILVER  — Limpeza e enriquecimento          |
|  (tipagem, filtragem, joins entre tabelas,   |
|   derivacao de colunas de negocio)           |
|  8 tabelas enriquecidas                      |
+---------------------+-----------------------+
                      |
                      v
+---------------------------------------------+
|  GOLD  — Agregacoes analiticas               |
|  (14 visoes para dashboards e relatorios)    |
|  faturamento, rankings, satisfacao,          |
|  segmentacao RFM, estoque, cupons            |
+---------------------------------------------+
```

## Estrutura do Projeto

```
spark_deltalake/
├── datasets/
│   └── gerar_dados.py              # Geracao de dados sinteticos (Faker)
├── notebooks/
│   ├── 01_bronze_ingestao.ipynb   # Ingestao dos CSVs para Delta Lake
│   ├── 02_silver_transformacao.ipynb  # Limpeza, joins e regras de negocio
│   └── 03_gold_agregacao.ipynb    # 14 agregacoes para analise
├── tools/
│   ├── spark_session.py           # Configuracao centralizada do Spark
│   ├── run_datagen.py             # Geracao de datasets via CLI
│   ├── run_bronze.py               # Ingestao Bronze (via spark-submit)
│   ├── run_silver.py              # Transformacao Silver (via spark-submit)
│   ├── run_gold.py                # Agregacao Gold (via spark-submit)
│   └── observabilidade.py          # Metricas Prometheus, logging, audit
├── airflow/dags/
│   └── gerar_dados_dag.py         # DAG Bronze -> Silver -> Gold
├── config/
│   └── prometheus.yml              # Scrape config (Pushgateway + Airflow)
├── grafana/
│   ├── provisioning/
│   │   ├── datasources/prometheus.yml
│   │   └── dashboards/dashboard.yml
│   └── dashboards/
│       └── pipeline-observabilidade.json
├── warehouse/
│   ├── bronze/                    # Dados brutos em Delta Lake
│   ├── silver/                    # Dados limpos em Delta Lake
│   ├── gold/                      # Agregacoes em Delta Lake
│   └── pipeline_audit/            # Tabela de auditoria das runs
├── docker-compose.yml             # Orquestracao dos containers
└── requirements.txt
```

## Tecnologias

| Componente | Tecnologia |
|---|---|
| Processamento | Apache Spark 4.0.0 |
| Storage | Delta Lake 4.0.0 |
| Orquestracao | Apache Airflow 3.0.2 |
| Metadados | PostgreSQL 16 |
| Metricas | Prometheus + Pushgateway |
| Visualizacao | Grafana |
| Geracao de Dados | Faker (locale pt_BR) |

## Dados Gerados

O script `gerar_dados.py` cria 8 arquivos CSV com timestamp no nome (ex: `clientes_20260621T013728.csv`) e dados simulados de e-commerce brasileiro:

| Tabela | Registros | Relacionamento |
|---|---|---|
| `fornecedores` | 50 | 1:N com produtos |
| `clientes` | 100.000 | 1:N com vendas |
| `produtos` | 1.000 | N:1 com fornecedores, 1:N com vendas |
| `cupons` | 200 | 1:N com vendas (nullable) |
| `vendas` | 1.000.000 | 1:1 com fretes e pagamentos |
| `fretes` | 1.000.000 | 1:1 com vendas |
| `pagamentos` | 1.000.000 | 1:1 com vendas |
| `avaliacoes` | ~420.000 | 1:1 com vendas (parcial) |

### Realismo dos dados

- **Precos por categoria**: faixas realistas em BRL (Alimentos R$5-150, Eletronicos R$100-5000, etc.)
- **Sazonalidade**: vendas concentradas em nov/dez (Black Friday + Natal), queda em janeiro
- **Documentos**: CPF para pessoa fisica (85%), CNPJ + razao social para pessoa juridica (15%)
- **Fretes por regiao**: Norte/Nordeste com fretes maiores e prazos mais longos
- **Pagamentos**: PIX 40%, cartao de credito 35%, boleto 15%, debito 10%

## Camadas

### Bronze — Ingestao Bruta

Leitura dos CSVs (com suporte a timestamp no nome do arquivo) e persistencia como Delta Lake sem nenhuma transformacao. Usa `glob` do Python para localizar arquivos por padrao de nome.

### Silver — Transformacao

8 tabelas com limpeza, tipagem, filtragem e enriquecimento:

| Tabela | Transformacoes |
|---|---|
| **clientes** | Limpa CPF/CNPJ, tipa datas/booleanos, `faixa_score` (bom/regular/ruim), agrega `qtd_compras` e `qtd_itens` |
| **produtos** | Tipa preco/estoque, `disponivel` (estoque > 0), join com fornecedores (razao_social, segmento) |
| **vendas** | Filtra CONCLUIDO, join com produtos/clientes/fretes/pagamentos, `valor_total`, `turno` (manha/tarde/noite), `ano_mes` |
| **fretes** | Filtra CANCELADO, tipa campos |
| **pagamentos** | Filtra PAGO, tipa campos, `eh_parcelado` |
| **cupons** | Tipa datas/valor, `ativo` (dentro da validade) |
| **avaliacoes** | Tipa campos, join com vendas e produtos |
| **fornecedores** | Limpa CNPJ, remove nulos |

### Gold — Agregacoes Analiticas

14 visoes prontas para consumo por dashboards e relatorios:

**Faturamento**

| Visao | Granularidade | Metricas |
|---|---|---|
| `receita_mensal` | mes | receita, qtd vendas, clientes unicos, ticket medio, receita frete |
| `receita_categoria` | categoria | receita, qtd vendas, qtd itens, receita frete |
| `receita_regiao` | regiao | receita, qtd vendas, clientes unicos, ticket medio |
| `receita_canal` | canal | receita, qtd vendas, clientes unicos, ticket medio, frete medio |

**Rankings**

| Visao | Granularidade | Metricas |
|---|---|---|
| `top_produtos` | produto | Top 20 por receita e qtd vendida |
| `top_clientes` | cliente | Top 20 por gasto e frequencia |
| `ranking_transportadoras` | transportadora | entregas, frete medio, prazo medio |

**Comportamento & Satisfacao**

| Visao | Metricas |
|---|---|
| `vendas_turno` | vendas e receita por turno (manha/tarde/noite) |
| `metodos_pagamento` | distribuicao por metodo, parcelas medias, % parcelado |
| `satisfacao_categoria` | nota media e qtd avaliacoes por categoria |
| `satisfacao_regiao` | nota media e qtd avaliacoes por regiao |

**Operacional**

| Visao | Metricas |
|---|---|
| `estoque_critico` | produtos sem estoque e fornecedor |
| `clientes_rfm` | segmentacao RFM (Recency, Frequency, Monetary) |
| `cupons_performance` | uso, receita gerada e ticket medio por cupom |

## Observabilidade

Métricas do pipeline sao exportadas para o Prometheus via Pushgateway e visualizadas no Grafana.

### Metricas coletadas

| Metrica | Labels | Descricao |
|---|---|---|
| `pipeline_fase_duration_seconds` | camada, tabela, fase, status | Duracao de cada fase |
| `pipeline_fase_row_count` | camada, tabela, fase, run_id | Linhas processadas (ultimas 3 runs) |
| `pipeline_camada_duration_seconds` | camada, status | Duracao total da camada |
| `pipeline_last_run_status` | camada | Status da ultima run (0=sucesso, 1=erro) |

### Dashboard Grafana

O dashboard `Pipeline E-commerce - Observabilidade` inclui:

- **Duracao por Camada** — timeseries com duracao por camada e status
- **Duracao por Tabela e Fase** — heatmap de duracao por fase
- **Volume por Camada (ultimas runs)** — tabela com volume das ultimas 3 execucoes
- **Status das Ultimas Runs** — stat com status por camada
- **SLA Breaches (24h)** — gauge de violacoes de SLA
- **Tendencia por Fase** — timeseries detalhada do bronze

### Auditoria

Tabela Delta `pipeline_audit` com registro de cada fase: run_id, timestamp, camada, tabela, fase, duracao_ms, row_count, status e erro_mensagem.

## Como Executar

### 1. Subir os containers

```bash
docker compose up -d
```

Containers criados: databricks-sim (Spark), Airflow, PostgreSQL, Prometheus, Pushgateway, Grafana.

### 2. Gerar dados de teste

```bash
python tools/run_datagen.py
```

Gera 8 CSVs em `datasets/` com dados sinteticos.

### 3. Executar o pipeline

**Via Airflow** (`http://localhost:8080`):
- Trigger manual da DAG `pipeline_ecommerce`
- Executa Bronze -> Silver -> Gold sequencialmente

**Via spark-submit direto**:
```bash
docker exec databricks-sim bash -c 'PYTHONPATH=/opt/spark/work-dir/tools spark-submit --packages io.delta:delta-spark_2.13:4.0.0 /opt/spark/work-dir/tools/run_bronze.py'
docker exec databricks-sim bash -c 'PYTHONPATH=/opt/spark/work-dir/tools spark-submit --packages io.delta:delta-spark_2.13:4.0.0 /opt/spark/work-dir/tools/run_silver.py'
docker exec databricks-sim bash -c 'PYTHONPATH=/opt/spark/work-dir/tools spark-submit --packages io.delta:delta-spark_2.13:4.0.0 /opt/spark/work-dir/tools/run_gold.py'
```

### 4. Acessar as interfaces

| Servico | URL | Credenciais |
|---|---|---|
| Grafana | http://localhost:3000 | admin / admin |
| Prometheus | http://localhost:9090 | — |
| Pushgateway | http://localhost:9091 | — |
| Airflow | http://localhost:8080 | admin / admin |
| Spark UI | http://localhost:4040 | — |

## Portas

| Servico | Porta |
|---|---|
| Grafana | 3000 |
| Prometheus | 9090 |
| Pushgateway | 9091 |
| Jupyter Notebook | 8888-8889 |
| Spark UI | 4040 |
| Airflow UI | 8080 |
