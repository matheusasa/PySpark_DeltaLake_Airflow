"""Script de transformacao Silver — chamado pela DAG Airflow."""
from pyspark.sql import functions as F
from pyspark.sql.types import DateType
from spark_session import get_spark
from observabilidade import medir_fase, RunContext

spark = get_spark('Silver - DAG')
spark.sparkContext.setLogLevel('WARN')

BRONZE = '/opt/spark/work-dir/warehouse/bronze'
SILVER = '/opt/spark/work-dir/warehouse/silver'

run_ctx = RunContext(camada="silver")
teve_erro = False

try:
    # Fornecedores
    with medir_fase("silver", "fornecedores", "leitura", run_ctx=run_ctx, spark=spark):
        fornecedores = (
            spark.read.format('delta').load(f'{BRONZE}/fornecedores')
            .withColumn('cnpj', F.regexp_replace(F.col('cnpj'), r'[^0-9]', ''))
            .filter(F.col('fornecedor_id').isNotNull())
        )
        count = fornecedores.count()

    with medir_fase("silver", "fornecedores", "escrita", run_ctx=run_ctx, spark=spark, row_count=count):
        fornecedores.write.format('delta').mode('overwrite').save(f'{SILVER}/fornecedores')

except Exception as e:
    teve_erro = True
    print(f'[silver/fornecedores] ERRO: {e}')

try:
    # Cupons
    with medir_fase("silver", "cupons", "leitura", run_ctx=run_ctx, spark=spark):
        cupons = (
            spark.read.format('delta').load(f'{BRONZE}/cupons')
            .withColumn('validade_inicio', F.col('validade_inicio').cast(DateType()))
            .withColumn('validade_fim', F.col('validade_fim').cast(DateType()))
            .withColumn('valor', F.col('valor').cast('double'))
            .withColumn('uso_minimo', F.col('uso_minimo').cast('double'))
            .withColumn('ativo',
                (F.current_date() >= F.col('validade_inicio'))
                & (F.current_date() <= F.col('validade_fim'))
            )
            .filter(F.col('cupom_id').isNotNull())
        )
        count = cupons.count()

    with medir_fase("silver", "cupons", "escrita", run_ctx=run_ctx, spark=spark, row_count=count):
        cupons.write.format('delta').mode('overwrite').save(f'{SILVER}/cupons')

except Exception as e:
    teve_erro = True
    print(f'[silver/cupons] ERRO: {e}')

try:
    # Fretes
    with medir_fase("silver", "fretes", "leitura", run_ctx=run_ctx, spark=spark):
        fretes = (
            spark.read.format('delta').load(f'{BRONZE}/fretes')
            .withColumn('valor_frete', F.col('valor_frete').cast('double'))
            .withColumn('prazo_dias', F.col('prazo_dias').cast('integer'))
            .filter(F.col('status_entrega') != 'CANCELADO')
            .filter(F.col('frete_id').isNotNull())
        )
        count = fretes.count()

    with medir_fase("silver", "fretes", "escrita", run_ctx=run_ctx, spark=spark, row_count=count):
        fretes.write.format('delta').mode('overwrite').save(f'{SILVER}/fretes')

except Exception as e:
    teve_erro = True
    print(f'[silver/fretes] ERRO: {e}')

try:
    # Pagamentos
    with medir_fase("silver", "pagamentos", "leitura", run_ctx=run_ctx, spark=spark):
        pagamentos = (
            spark.read.format('delta').load(f'{BRONZE}/pagamentos')
            .withColumn('valor_pago', F.col('valor_pago').cast('double'))
            .withColumn('parcelas', F.col('parcelas').cast('integer'))
            .withColumn('eh_parcelado', F.col('parcelas') > 1)
            .filter(F.col('status_pagamento') == 'PAGO')
            .filter(F.col('pagamento_id').isNotNull())
        )
        count = pagamentos.count()

    with medir_fase("silver", "pagamentos", "escrita", run_ctx=run_ctx, spark=spark, row_count=count):
        pagamentos.write.format('delta').mode('overwrite').save(f'{SILVER}/pagamentos')

except Exception as e:
    teve_erro = True
    print(f'[silver/pagamentos] ERRO: {e}')

try:
    # Clientes — com agregacoes de compra
    with medir_fase("silver", "clientes", "leitura", run_ctx=run_ctx, spark=spark):
        vendas_agg = (
            spark.read.format('delta').load(f'{BRONZE}/vendas')
            .filter(F.col('status') == 'CONCLUIDO')
            .groupBy('cliente_id')
            .agg(
                F.count('venda_id').alias('qtd_compras'),
                F.sum('quantidade').alias('qtd_itens'),
            )
        )

        clientes = (
            spark.read.format('delta').load(f'{BRONZE}/clientes')
            .withColumn('documento', F.regexp_replace(F.col('documento'), r'[^0-9]', ''))
            .withColumn('data_cadastro', F.col('data_cadastro').cast(DateType()))
            .withColumn('ativo', F.col('ativo').cast('boolean'))
            .withColumn('faixa_score',
                F.when(F.col('score_credito') >= 700, 'bom')
                 .when(F.col('score_credito') >= 500, 'regular')
                 .otherwise('ruim')
            )
            .filter(F.col('cliente_id').isNotNull())
            .join(vendas_agg, 'cliente_id', 'left')
        )
        count = clientes.count()

    with medir_fase("silver", "clientes", "escrita", run_ctx=run_ctx, spark=spark, row_count=count):
        clientes.write.format('delta').mode('overwrite').save(f'{SILVER}/clientes')

except Exception as e:
    teve_erro = True
    print(f'[silver/clientes] ERRO: {e}')

try:
    # Produtos — com fornecedor
    with medir_fase("silver", "produtos", "leitura", run_ctx=run_ctx, spark=spark):
        produtos = (
            spark.read.format('delta').load(f'{BRONZE}/produtos')
            .withColumn('preco', F.col('preco').cast('double'))
            .withColumn('estoque', F.col('estoque').cast('integer'))
            .withColumn('disponivel', F.col('estoque') > 0)
            .join(
                fornecedores.select('fornecedor_id', 'razao_social', 'segmento'),
                'fornecedor_id', 'left'
            )
        )
        count = produtos.count()

    with medir_fase("silver", "produtos", "escrita", run_ctx=run_ctx, spark=spark, row_count=count):
        produtos.write.format('delta').mode('overwrite').save(f'{SILVER}/produtos')

except Exception as e:
    teve_erro = True
    print(f'[silver/produtos] ERRO: {e}')

try:
    # Vendas — com todas as dimensoes
    with medir_fase("silver", "vendas", "leitura", run_ctx=run_ctx, spark=spark):
        vendas = (
            spark.read.format('delta').load(f'{BRONZE}/vendas')
            .withColumn('data_venda', F.col('data_venda').cast(DateType()))
            .withColumn('quantidade', F.col('quantidade').cast('integer'))
            .withColumn('desconto_pct', F.col('desconto_pct').cast('double'))
            .withColumn('turno',
                F.when(F.hour(F.col('hora_venda')) < 12, 'manhã')
                 .when(F.hour(F.col('hora_venda')) < 18, 'tarde')
                 .otherwise('noite')
            )
            .filter(F.col('status') == 'CONCLUIDO')
            .join(produtos.select('produto_id', 'preco', 'nome', 'categoria'), 'produto_id')
            .join(clientes.select('cliente_id', 'regiao', 'faixa_score'), 'cliente_id')
            .join(fretes.select('venda_id', 'valor_frete', 'prazo_dias', 'transportadora', 'status_entrega'), 'venda_id')
            .join(pagamentos.select('venda_id', 'metodo', 'parcelas'), 'venda_id')
            .withColumn(
                'valor_total',
                F.round(F.col('preco') * F.col('quantidade') * (1 - F.col('desconto_pct') / 100), 2)
            )
            .withColumn('ano_mes', F.date_format('data_venda', 'yyyy-MM'))
        )
        count = vendas.count()

    with medir_fase("silver", "vendas", "escrita", run_ctx=run_ctx, spark=spark, row_count=count):
        vendas.write.format('delta').mode('overwrite').save(f'{SILVER}/vendas')

except Exception as e:
    teve_erro = True
    print(f'[silver/vendas] ERRO: {e}')

try:
    # Avaliacoes
    with medir_fase("silver", "avaliacoes", "leitura", run_ctx=run_ctx, spark=spark):
        avaliacoes = (
            spark.read.format('delta').load(f'{BRONZE}/avaliacoes')
            .withColumn('data', F.col('data').cast(DateType()))
            .withColumn('nota', F.col('nota').cast('integer'))
            .filter(F.col('avaliacao_id').isNotNull())
            .join(
                vendas.select('venda_id', 'cliente_id', 'produto_id', 'valor_total', 'ano_mes'),
                'venda_id', 'left'
            )
            .join(
                produtos.select('produto_id', F.col('nome').alias('produto_nome'), 'categoria'),
                'produto_id', 'left'
            )
        )
        count = avaliacoes.count()

    with medir_fase("silver", "avaliacoes", "escrita", run_ctx=run_ctx, spark=spark, row_count=count):
        avaliacoes.write.format('delta').mode('overwrite').save(f'{SILVER}/avaliacoes')

except Exception as e:
    teve_erro = True
    print(f'[silver/avaliacoes] ERRO: {e}')

metrics = run_ctx.finalizar()
print(f'\nSilver: transformacao finalizada (run_id={run_ctx.run_id}, status={metrics["status"]})')
spark.stop()

exit(1 if teve_erro else 0)
