"""Script standalone para gerar datasets sinteticos do e-commerce.

Uso:
    python tools/run_datagen.py
    python tools/run_datagen.py --output-dir /caminho/custom
"""
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "datasets"))
from gerar_dados import (
    gerar_fornecedores,
    gerar_clientes,
    gerar_cupons,
    gerar_produtos,
    gerar_vendas_fretes_pagamentos,
    gerar_avaliacoes,
)

DEFAULT_OUTPUT = str(Path(__file__).resolve().parent.parent / "datasets")


if __name__ == "__main__":
    output_dir = DEFAULT_OUTPUT
    for arg in sys.argv[1:]:
        if arg.startswith("--output-dir="):
            output_dir = arg.split("=", 1)[1]

    ts = datetime.now().strftime("%Y%m%dT%H%M%S")

    gerar_fornecedores(output_dir=output_dir, run_timestamp=ts)
    gerar_clientes(output_dir=output_dir, run_timestamp=ts)
    gerar_cupons(output_dir=output_dir, run_timestamp=ts)
    gerar_produtos(output_dir=output_dir, run_timestamp=ts)
    gerar_vendas_fretes_pagamentos(output_dir=output_dir, run_timestamp=ts)
    gerar_avaliacoes(output_dir=output_dir, run_timestamp=ts)

    print(f"\nDatasets gerados (run_timestamp={ts}):")
    for csv_file in sorted(Path(output_dir).glob(f"*_{ts}.csv")):
        size_mb = csv_file.stat().st_size / 1_048_576
        print(f"  {csv_file.name:<35} {size_mb:>8.1f} MB")
