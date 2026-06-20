# Spark Delta Lake - Pipeline de Dados E-commerce

Pipeline de dados implementando a **arquitetura medallion** (Bronze → Silver → Gold) com Apache Spark e Delta Lake para processamento de dados de e-commerce.

## Arquitetura

```
datasets/ (CSVs brutos)
    │
    ▼
┌─────────────────────────────────────────┐
│  BRONZE  — Ingestao de dados brutos     │
│  (schema original, sem transformacao)   │
└──────────────┬──────────────────────────┘
               ▼
┌─────────────────────────────────────────┐
│  SILVER  — Limpeza e enriquecimento     │
│  (tipagem, filtragem, joins,            │
│   calculo de valor_total)               │
└──────────────┬──────────────────────────┘
               ▼
┌─────────────────────────────────────────┐
│  GOLD  — Tabelas analiticas             │
│  (receita mensal, por categoria,        │
│   top clientes, por regiao)             │
└─────────────────────────────────────────┘
```

## Estrutura do Projeto

```
spark_deltalake/
├── datasets/
│   └── gerar_dados.py            # Geracao de dados sinteticos (Faker)
├── notebooks/
│   ├── 01_bronze_ingestao.ipynb  # Ingestao dos CSVs para Delta Lake
│   ├── 02_silver_transformacao.ipynb  # Limpeza, joins e regras de negocio
│   └── 03_gold_agregacao.ipynb   # Agregacoes para analise
├── tools/
│   └── spark_session.py         # Configuracao centralizada do Spark
├── warehouse/
│   ├── bronze/                   # Dados brutos em Delta Lake
│   ├── silver/                   # Dados limpos em Delta Lake
│   └── gold/                     # Agregacoes em Delta Lake
├── docker-compose.yml            # Orquestracao dos containers
└── .gitignore
```

## Tecnologias

| Componente | Tecnologia |
|---|---|
| Processamento | Apache Spark 4.0.0 |
| Storage | Delta Lake 4.0.0 |
| Orquestracao | Apache Airflow 3.0.2 |
| Metadados | PostgreSQL 16 |
| Geracao de Dados | Faker (locale pt_BR) |

## Dados Gerados

O script `gerar_dados.py` cria 8 arquivos CSV com dados simulados de e-commerce brasileiro:

| Arquivo | Registros | Campos principais |
|---|---|---|
| `fornecedores.csv` | 50 | razao social, CNPJ, regiao, contato, segmento |
| `clientes.csv` | 100.000 | nome, tipo (PF/PJ), CPF/CNPJ, regiao, score credito |
| `produtos.csv` | 1.000 | nome, categoria, preco (faixa por categoria), fornecedor |
| `cupons.csv` | 200 | codigo, tipo (fixo/%), valor, validade, uso minimo |
| `vendas.csv` | 1.000.000 | cliente, produto, quantidade, cupom, status, canal, data |
| `fretes.csv` | 1.000.000 | valor, prazo, transportadora, status entrega |
| `pagamentos.csv` | 1.000.000 | metodo (PIX/cartao/boleto), parcelas, valor, status |
| `avaliacoes.csv` | ~420.000 | nota (1-5), comentario, data |

### Realismo dos dados

- **Precos por categoria**: faixas realistas em BRL (Alimentos R$5-150, Eletronicos R$100-5000, etc.)
- **Sazonalidade**: vendas concentradas em nov/dez (Black Friday + Natal), queda em janeiro
- **Documentos**: CPF para pessoa fisica (85%), CNPJ + razao social para pessoa juridica (15%)
- **Distribuicao de quantidade**: maioria das compras tem 1-3 itens
- **Fretes por regiao**: Norte/Nordeste com fretes maiores e prazos mais longos

## Camadas

### Bronze — Ingestao Bruta
Leitura dos CSVs e persistencia como tabelas Delta Lake sem nenhuma transformacao.

### Silver — Transformacao
- Conversao de tipos (datas, booleanos, numericos)
- Filtragem de vendas com status `CONCLUIDO`
- Remocao de registros com cliente nulo
- Join com dimensoes de cliente e produto
- Calculo de `valor_total` (preco x quantidade x fator desconto)
- Derivacao de `disponivel` e `ano_mes`

### Gold — Agregacoes
Quatro tabelas analiticas:
- **receita_mensal** — receita, quantidade e ticket medio por mes
- **receita_categoria** — receita por categoria de produto
- **top_clientes** — top 5 clientes por volume de compra
- **receita_regiao** — receita por regiao geografica

## Como Executar

### 1. Subir os containers

```bash
docker compose up -d
```

### 2. Acessar o ambiente Spark

```bash
docker exec -it databricks-sim bash
```

Dentro do container, os notebooks ficam em `/opt/spark/work-dir/notebooks/`.

### 3. Gerar dados de teste

```bash
cd /opt/spark/work-dir/datasets
python gerar_dados.py
```

### 4. Executar os notebooks

Execute na ordem: `01_bronze_ingestao` → `02_silver_transformacao` → `03_gold_agregacao`.

### 5. Interface Airflow (opcional)

Acesse `http://localhost:8080` para o Airflow UI.

## Portas

| Servico | Porta |
|---|---|
| Jupyter Notebook | 8888-8889 |
| Spark UI | 4040 |
| Airflow UI | 8080 |
