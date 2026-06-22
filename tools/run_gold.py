"""Script de agregacao Gold — chamado pela DAG Airflow."""
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from spark_session import get_spark
from observabilidade import medir_fase, RunContext

spark = get_spark('Gold - DAG')
spark.sparkContext.setLogLevel('WARN')

SILVER = '/opt/spark/work-dir/warehouse/silver'
GOLD   = '/opt/spark/work-dir/warehouse/gold'

run_ctx = RunContext(camada="gold")
teve_erro = False

with medir_fase("gold", "_silver_read", "leitura", run_ctx=run_ctx, spark=spark):
    vendas      = spark.read.format('delta').load(f'{SILVER}/vendas')
    clientes    = spark.read.format('delta').load(f'{SILVER}/clientes')
    produtos    = spark.read.format('delta').load(f'{SILVER}/produtos')
    cupons      = spark.read.format('delta').load(f'{SILVER}/cupons')
    avaliacoes  = spark.read.format('delta').load(f'{SILVER}/avaliacoes')

# Faturamento
with medir_fase("gold", "receita_mensal", "transformacao", run_ctx=run_ctx, spark=spark):
    receita_mensal = (
        vendas.groupBy('ano_mes')
        .agg(
            F.count('venda_id').alias('qtd_vendas'),
            F.countDistinct('cliente_id').alias('qtd_clientes_unicos'),
            F.round(F.sum('valor_total'), 2).alias('receita_total'),
            F.round(F.avg('valor_total'), 2).alias('ticket_medio'),
            F.round(F.sum('valor_frete'), 2).alias('receita_frete'),
        )
        .orderBy('ano_mes')
    )

with medir_fase("gold", "receita_categoria", "transformacao", run_ctx=run_ctx, spark=spark):
    receita_categoria = (
        vendas.groupBy('categoria')
        .agg(
            F.count('venda_id').alias('qtd_vendas'),
            F.sum('quantidade').alias('qtd_itens'),
            F.round(F.sum('valor_total'), 2).alias('receita_total'),
            F.round(F.sum('valor_frete'), 2).alias('receita_frete'),
        )
        .orderBy(F.desc('receita_total'))
    )

with medir_fase("gold", "receita_regiao", "transformacao", run_ctx=run_ctx, spark=spark):
    receita_regiao = (
        vendas.groupBy('regiao')
        .agg(
            F.count('venda_id').alias('qtd_vendas'),
            F.countDistinct('cliente_id').alias('qtd_clientes_unicos'),
            F.round(F.sum('valor_total'), 2).alias('receita_total'),
            F.round(F.avg('valor_total'), 2).alias('ticket_medio'),
        )
        .orderBy(F.desc('receita_total'))
    )

with medir_fase("gold", "receita_canal", "transformacao", run_ctx=run_ctx, spark=spark):
    receita_canal = (
        vendas.groupBy('canal')
        .agg(
            F.count('venda_id').alias('qtd_vendas'),
            F.countDistinct('cliente_id').alias('qtd_clientes_unicos'),
            F.round(F.sum('valor_total'), 2).alias('receita_total'),
            F.round(F.avg('valor_total'), 2).alias('ticket_medio'),
            F.round(F.avg('valor_frete'), 2).alias('frete_medio'),
        )
        .orderBy(F.desc('receita_total'))
    )

# Rankings
with medir_fase("gold", "top_produtos", "transformacao", run_ctx=run_ctx, spark=spark):
    top_produtos = (
        vendas.groupBy('produto_id', 'nome', 'categoria')
        .agg(
            F.count('venda_id').alias('qtd_vendas'),
            F.sum('quantidade').alias('qtd_itens'),
            F.round(F.sum('valor_total'), 2).alias('receita_total'),
            F.round(F.avg('valor_total'), 2).alias('ticket_medio'),
        )
        .orderBy(F.desc('receita_total'))
        .limit(20)
    )

with medir_fase("gold", "top_clientes", "transformacao", run_ctx=run_ctx, spark=spark):
    clientes_nome = clientes.select('cliente_id', F.col('nome').alias('nome_cliente'))
    top_clientes = (
        vendas.join(clientes_nome, 'cliente_id')
        .groupBy('cliente_id', 'nome_cliente')
        .agg(
            F.count('venda_id').alias('qtd_compras'),
            F.round(F.sum('valor_total'), 2).alias('total_gasto'),
            F.round(F.avg('valor_total'), 2).alias('ticket_medio'),
        )
        .orderBy(F.desc('total_gasto'))
        .limit(20)
    )

with medir_fase("gold", "ranking_transportadoras", "transformacao", run_ctx=run_ctx, spark=spark):
    ranking_transportadoras = (
        vendas.groupBy('transportadora')
        .agg(
            F.count('venda_id').alias('qtd_entregas'),
            F.round(F.avg('valor_frete'), 2).alias('frete_medio'),
            F.round(F.avg('prazo_dias'), 2).alias('prazo_medio_dias'),
            F.round(F.sum('valor_frete'), 2).alias('receita_frete'),
        )
        .orderBy(F.desc('qtd_entregas'))
    )

# Comportamento & Satisfacao
with medir_fase("gold", "vendas_turno", "transformacao", run_ctx=run_ctx, spark=spark):
    vendas_turno = (
        vendas.groupBy('turno')
        .agg(
            F.count('venda_id').alias('qtd_vendas'),
            F.round(F.sum('valor_total'), 2).alias('receita_total'),
            F.round(F.avg('valor_total'), 2).alias('ticket_medio'),
        )
        .orderBy('turno')
    )

with medir_fase("gold", "metodos_pagamento", "transformacao", run_ctx=run_ctx, spark=spark):
    metodos_pagamento = (
        vendas.groupBy('metodo')
        .agg(
            F.count('venda_id').alias('qtd_vendas'),
            F.round(F.sum('valor_total'), 2).alias('receita_total'),
            F.round(F.avg('parcelas'), 2).alias('parcelas_medio'),
            F.round(
                F.count(F.when(F.col('parcelas') > 1, F.lit(1))) * 100.0 / F.count('venda_id'), 2
            ).alias('pct_parcelada'),
        )
        .orderBy(F.desc('receita_total'))
    )

with medir_fase("gold", "satisfacao_categoria", "transformacao", run_ctx=run_ctx, spark=spark):
    satisfacao_categoria = (
        avaliacoes.groupBy('categoria')
        .agg(
            F.count('avaliacao_id').alias('qtd_avaliacoes'),
            F.round(F.avg('nota'), 2).alias('nota_media'),
        )
        .orderBy(F.desc('nota_media'))
    )

with medir_fase("gold", "satisfacao_regiao", "transformacao", run_ctx=run_ctx, spark=spark):
    satisfacao_regiao = (
        avaliacoes
        .join(clientes.select('cliente_id', 'regiao'), 'cliente_id')
        .groupBy('regiao')
        .agg(
            F.count('avaliacao_id').alias('qtd_avaliacoes'),
            F.round(F.avg('nota'), 2).alias('nota_media'),
        )
        .orderBy(F.desc('nota_media'))
    )

# Operacional
with medir_fase("gold", "estoque_critico", "transformacao", run_ctx=run_ctx, spark=spark):
    estoque_critico = (
        produtos
        .filter(F.col('disponivel') == False)
        .select('produto_id', 'nome', 'categoria', 'estoque', 'razao_social')
        .orderBy('estoque')
    )

with medir_fase("gold", "clientes_rfm", "transformacao", run_ctx=run_ctx, spark=spark):
    clientes_rfm = (
        vendas
        .groupBy('cliente_id')
        .agg(
            F.datediff(F.current_date(), F.max('data_venda')).alias('recencia_dias'),
            F.count('venda_id').alias('frequencia'),
            F.round(F.sum('valor_total'), 2).alias('monetario'),
        )
        .join(clientes.select('cliente_id', 'nome', 'regiao', 'faixa_score'), 'cliente_id')
        .orderBy(F.desc('monetario'))
    )

with medir_fase("gold", "cupons_performance", "transformacao", run_ctx=run_ctx, spark=spark):
    cupons_performance = (
        vendas
        .filter(F.col('cupom_id').isNotNull())
        .groupBy('cupom_id')
        .agg(
            F.count('venda_id').alias('qtd_usos'),
            F.round(F.sum('valor_total'), 2).alias('receita_com_cupom'),
            F.round(F.avg('valor_total'), 2).alias('ticket_medio'),
        )
        .join(cupons.select('cupom_id', 'codigo', 'tipo', 'valor', 'categoria'), 'cupom_id')
        .orderBy(F.desc('qtd_usos'))
    )

# Persiste
for name, df in [
    ('receita_mensal', receita_mensal), ('receita_categoria', receita_categoria),
    ('receita_regiao', receita_regiao), ('receita_canal', receita_canal),
    ('top_produtos', top_produtos), ('top_clientes', top_clientes),
    ('ranking_transportadoras', ranking_transportadoras), ('vendas_turno', vendas_turno),
    ('metodos_pagamento', metodos_pagamento), ('satisfacao_categoria', satisfacao_categoria),
    ('satisfacao_regiao', satisfacao_regiao), ('estoque_critico', estoque_critico),
    ('clientes_rfm', clientes_rfm), ('cupons_performance', cupons_performance),
]:
    with medir_fase("gold", name, "escrita", run_ctx=run_ctx, spark=spark, row_count=df.count() if df else 0):
        df.write.format('delta').mode('overwrite').save(f'{GOLD}/{name}')

# ---- Tabelas Gold de Observabilidade ----
with medir_fase("gold", "_observabilidade", "transformacao", run_ctx=run_ctx, spark=spark):
    audit_path = '/opt/spark/work-dir/warehouse/pipeline_audit'

    try:
        audit_df = spark.read.format('delta').load(audit_path).filter(F.col('status') == 'sucesso')

        w = Window.partitionBy('camada', 'tabela', 'fase').orderBy(F.col('timestamp').desc())

        duracao_fase_tabela = (
            audit_df
            .withColumn('rn', F.row_number().over(w))
            .filter(F.col('rn') <= 30)
            .groupBy('camada', 'tabela', 'fase')
            .agg(
                F.round(F.avg('duracao_ms'), 2).alias('duracao_media_ms'),
                F.min('duracao_ms').alias('duracao_min_ms'),
                F.max('duracao_ms').alias('duracao_max_ms'),
                F.round(F.expr('percentile_approx(duracao_ms, 0.95)'), 2).alias('duracao_p95_ms'),
                F.count('*').alias('qtd_runs'),
            )
        )
        duracao_fase_tabela.write.format('delta').mode('overwrite').save(f'{GOLD}/duracao_fase_tabela')

        duracao_camada_total = (
            audit_df
            .groupBy('camada', 'run_id')
            .agg(F.sum('duracao_ms').alias('duracao_total_ms'))
            .withColumn('run_ts', F.max('timestamp').over(Window.partitionBy('camada').orderBy('run_id')))
        )

        w_camada = Window.partitionBy('camada').orderBy(F.col('run_ts').desc())
        duracao_camada_total = (
            duracao_camada_total
            .withColumn('rn', F.row_number().over(w_camada))
            .withColumn('duracao_anterior',
                F.lag('duracao_total_ms').over(w_camada)
            )
            .filter(F.col('rn') == 1)
            .withColumn('variacao_vs_run_anterior_pct',
                F.round((F.col('duracao_total_ms') - F.col('duracao_anterior')) / F.col('duracao_anterior') * 100, 2)
            )
            .select('camada', 'duracao_total_ms', 'variacao_vs_run_anterior_pct')
        )
        duracao_camada_total.write.format('delta').mode('overwrite').save(f'{GOLD}/duracao_camada_total')

        volume_dados_por_tabela = (
            audit_df
            .filter(F.col('row_count') > 0)
            .groupBy('camada', 'tabela', 'run_id')
            .agg(F.max('row_count').alias('row_count'))
        )
        w_vol = Window.partitionBy('camada', 'tabela').orderBy('run_id')
        volume_dados_por_tabela = (
            volume_dados_por_tabela
            .withColumn('row_count_anterior', F.lag('row_count').over(w_vol))
            .withColumn('variacao_vs_run_anterior_pct',
                F.round((F.col('row_count') - F.col('row_count_anterior')) / F.col('row_count_anterior') * 100, 2)
            )
            .filter(F.col('rn') == 1)
        )
        volume_dados_por_tabela.write.format('delta').mode('overwrite').save(f'{GOLD}/volume_dados_por_tabela')

    except Exception as e:
        print(f'[gold/observabilidade] Aviso: tabela de audit ainda nao existe ({e}). Tabelas de observabilidade serao geradas apos o primeiro run.')

metrics = run_ctx.finalizar()
print(f'\nGold: agregacao finalizada (run_id={run_ctx.run_id}, status={metrics["status"]})')
spark.stop()

exit(1 if teve_erro else 0)
