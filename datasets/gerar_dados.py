"""Gera CSVs de exemplo na pasta datasets/ com volume realista para testes Spark."""
import csv
import random
from datetime import datetime, timedelta
from pathlib import Path
from faker import Faker

HERE = Path(__file__).parent
rng = random.Random(42)
fake = Faker('pt_BR')
fake.seed_instance(42)

# ── Configurações de volume ──────────────────────────────────────────────────
N_CLIENTES = 100_000
N_PRODUTOS  = 1_000
N_VENDAS    = 1_000_000

# ── Domínios ─────────────────────────────────────────────────────────────────
CATEGORIAS  = ["Eletrônicos", "Roupas", "Alimentos", "Livros", "Esportes", "Casa", "Beleza", "Automotivo"]
REGIOES     = ["Sul", "Sudeste", "Norte", "Nordeste", "Centro-Oeste"]
STATUS      = ["CONCLUIDO", "CONCLUIDO", "CONCLUIDO", "CANCELADO", "PENDENTE"]
DESCONTOS   = [0, 0, 0, 5, 5, 10, 15, 20]

BASE_DATE = datetime(2022, 1, 1)
RANGE_DIAS = 900  # ~2,5 anos de histórico


def _data(offset_dias: int) -> str:
    return (BASE_DATE + timedelta(days=offset_dias)).strftime("%Y-%m-%d")


def _rand_nome() -> str:
    return fake.name()


# ── clientes.csv ─────────────────────────────────────────────────────────────
print(f"Gerando clientes.csv  ({N_CLIENTES:,} linhas)...")
with open(HERE / "clientes.csv", "w", newline="", encoding="utf-8", buffering=1 << 20) as f:
    w = csv.writer(f)
    w.writerow(["cliente_id", "nome", "regiao", "cidade", "data_cadastro", "ativo", "score_credito"])
    CIDADES = {
        "Sul":          ["Curitiba","Porto Alegre","Florianópolis","Caxias do Sul","Joinville"],
        "Sudeste":      ["São Paulo","Rio de Janeiro","Belo Horizonte","Campinas","Santos"],
        "Norte":        ["Manaus","Belém","Porto Velho","Macapá","Boa Vista"],
        "Nordeste":     ["Salvador","Recife","Fortaleza","Natal","Maceió"],
        "Centro-Oeste": ["Brasília","Goiânia","Campo Grande","Cuiabá","Palmas"],
    }
    for i in range(1, N_CLIENTES + 1):
        regiao = rng.choice(REGIOES)
        w.writerow([
            i,
            _rand_nome(),
            regiao,
            rng.choice(CIDADES[regiao]),
            _data(rng.randint(0, RANGE_DIAS)),
            rng.choices([1, 0], weights=[85, 15])[0],
            round(rng.uniform(300, 1000), 1),
        ])

# ── produtos.csv ─────────────────────────────────────────────────────────────
print(f"Gerando produtos.csv  ({N_PRODUTOS:,} linhas)...")
PREFIXOS = {
    "Eletrônicos": ["Smartphone","Notebook","Tablet","Fone","Monitor","Teclado","Mouse","Câmera","Smart TV","Carregador"],
    "Roupas":      ["Camiseta","Calça","Moletom","Jaqueta","Bermuda","Vestido","Saia","Polo","Blazer","Short"],
    "Alimentos":   ["Arroz","Feijão","Azeite","Café","Açúcar","Macarrão","Molho","Granola","Proteína","Chá"],
    "Livros":      ["Manual de","Guia de","Introdução a","O Poder do","Princípios de","Arte de","Fundamentos de","Dominando","Python e","Spark:"],
    "Esportes":    ["Tênis","Bicicleta","Haltere","Corda","Luva","Capacete","Joelheira","Garrafa","Mochila","Esteira"],
    "Casa":        ["Sofá","Tapete","Luminária","Panela","Aspirador","Ventilador","Porta-retratos","Travesseiro","Lençol","Vaso"],
    "Beleza":      ["Shampoo","Condicionador","Creme","Perfume","Sérum","Protetor","Batom","Delineador","Máscara","Esfoliante"],
    "Automotivo":  ["Pneu","Óleo","Filtro","Limpador","Película","Capa","Carregador Veicular","GPS","Kit Ferramentas","Tapete Auto"],
}
with open(HERE / "produtos.csv", "w", newline="", encoding="utf-8", buffering=1 << 20) as f:
    w = csv.writer(f)
    w.writerow(["produto_id", "nome", "categoria", "preco", "estoque", "peso_kg", "fornecedor_id"])
    for i in range(1, N_PRODUTOS + 1):
        cat = rng.choice(CATEGORIAS)
        prefixo = rng.choice(PREFIXOS[cat])
        modelo = rng.randint(100, 999)
        w.writerow([
            i,
            f"{prefixo} {modelo}",
            cat,
            round(rng.uniform(5, 5000), 2),
            rng.randint(0, 500),
            round(rng.uniform(0.1, 30), 2),
            rng.randint(1, 50),
        ])

# ── vendas.csv ───────────────────────────────────────────────────────────────
print(f"Gerando vendas.csv    ({N_VENDAS:,} linhas)...")
CANAIS = ["online", "online", "loja_fisica", "app", "televendas"]
with open(HERE / "vendas.csv", "w", newline="", encoding="utf-8", buffering=4 << 20) as f:
    w = csv.writer(f)
    w.writerow(["venda_id", "cliente_id", "produto_id", "quantidade", "desconto_pct",
                "status", "canal", "data_venda", "hora_venda"])
    BATCH = 10_000
    rows = []
    for venda_id in range(1, N_VENDAS + 1):
        hora = f"{rng.randint(6,22):02d}:{rng.randint(0,59):02d}:{rng.randint(0,59):02d}"
        rows.append([
            venda_id,
            rng.randint(1, N_CLIENTES),
            rng.randint(1, N_PRODUTOS),
            rng.randint(1, 10),
            rng.choice(DESCONTOS),
            rng.choice(STATUS),
            rng.choice(CANAIS),
            _data(rng.randint(0, RANGE_DIAS)),
            hora,
        ])
        if len(rows) == BATCH:
            w.writerows(rows)
            rows.clear()
    if rows:
        w.writerows(rows)

print("\nDatasets gerados:")
for csv_file in sorted(HERE.glob("*.csv")):
    size_mb = csv_file.stat().st_size / 1_048_576
    print(f"  {csv_file.name:<20} {size_mb:>8.1f} MB")
