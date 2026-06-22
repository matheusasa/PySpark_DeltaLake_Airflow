"""Script de ingestao Bronze — chamado pela DAG Airflow."""
import glob as glob_mod
from spark_session import get_spark
from observabilidade import medir_fase, RunContext

spark = get_spark('Bronze - DAG')
spark.sparkContext.setLogLevel('WARN')

DATASETS = '/opt/spark/work-dir/datasets'
BRONZE   = '/opt/spark/work-dir/warehouse/bronze'

run_ctx = RunContext(camada="bronze")
teve_erro = False

for table in ['clientes', 'produtos', 'vendas', 'fretes', 'pagamentos', 'cupons', 'avaliacoes', 'fornecedores']:
    try:
        with medir_fase("bronze", table, "leitura", run_ctx=run_ctx, spark=spark):
            files = sorted(glob_mod.glob(f'{DATASETS}/{table}_*.csv'))
            if not files:
                print(f'[bronze/{table}] NENHUM arquivo encontrado — pulando')
                continue
            df = (
                spark.read
                .option('header', True)
                .option('inferSchema', True)
                .csv(files[0])
            )
            count = df.count()

        with medir_fase("bronze", table, "escrita", run_ctx=run_ctx, spark=spark, row_count=count):
            df.write.format('delta').mode('overwrite').save(f'{BRONZE}/{table}')

    except Exception as e:
        teve_erro = True
        print(f'[bronze/{table}] ERRO: {e}')

metrics = run_ctx.finalizar()
print(f'\nBronze: ingestao finalizada (run_id={run_ctx.run_id}, status={metrics["status"]})')
spark.stop()

exit(1 if teve_erro else 0)
