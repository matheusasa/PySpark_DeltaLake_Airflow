"""Gera CSVs de exemplo na pasta datasets/ com volume realista para testes Spark."""
import csv
import random
from datetime import datetime, timedelta
from pathlib import Path
from faker import Faker

HERE = Path(__file__).parent
rng = random.Random(42)
fake = Faker("pt_BR")
fake.seed_instance(42)

# ── Configurações de volume ──────────────────────────────────────────
N_FORNECEDORES = 50
N_CLIENTES     = 100_000
N_PRODUTOS     = 1_000
N_CUPONS       = 200
N_VENDAS       = 1_000_000

# ── Domínios ─────────────────────────────────────────────────────────
CATEGORIAS = ["Eletrônicos", "Roupas", "Alimentos", "Livros", "Esportes",
              "Casa", "Beleza", "Automotivo"]

REGIOES = ["Sul", "Sudeste", "Norte", "Nordeste", "Centro-Oeste"]

CIDADES = {
    "Sul":          ["Curitiba","Porto Alegre","Florianópolis","Caxias do Sul","Joinville"],
    "Sudeste":      ["São Paulo","Rio de Janeiro","Belo Horizonte","Campinas","Santos"],
    "Norte":        ["Manaus","Belém","Porto Velho","Macapá","Boa Vista"],
    "Nordeste":     ["Salvador","Recife","Fortaleza","Natal","Maceió"],
    "Centro-Oeste": ["Brasília","Goiânia","Campo Grande","Cuiabá","Palmas"],
}

STATUS  = ["CONCLUIDO", "CONCLUIDO", "CONCLUIDO", "CANCELADO", "PENDENTE"]
CANAIS  = ["online", "online", "loja_fisica", "app", "televendas"]

BASE_DATE  = datetime(2022, 1, 1)
RANGE_DIAS = 900  # ~2,5 anos de histórico

PRECO_RANGES = {
    "Eletrônicos": (100, 5000),
    "Roupas":      (20, 200),
    "Alimentos":   (5, 150),
    "Livros":      (20, 200),
    "Esportes":    (30, 800),
    "Casa":        (10, 300),
    "Beleza":      (10, 300),
    "Automotivo":  (50, 2000),
}

# Pesos por mês (1=Jan .. 12=Dez) — sazonalidade
MONTH_WEIGHTS = {
    1: 5, 2: 7, 3: 9, 4: 8, 5: 10, 6: 7,
    7: 7, 8: 8, 9: 8, 10: 9, 11: 15, 12: 15,
}

DESCONTOS        = [0, 0, 0, 5, 5, 10, 15, 20]
TRANSPORTADORAS  = ["Correios", "Jadlog", "Total Express", "Braspress"]
SEGMENTOS       = ["Eletrônicos", "Alimentos", "Têxtil", "Cosméticos", "Livros",
                   "Esportivos", "Casa e Construção", "Automotivo", "Logística", "Diversos"]

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


# ── Helpers ─────────────────────────────────────────────────────────
def _data(offset_dias: int) -> str:
    return (BASE_DATE + timedelta(days=offset_dias)).strftime("%Y-%m-%d")


# Pre-compute seasonal day weights
_day_weights  = [MONTH_WEIGHTS[(BASE_DATE + timedelta(days=d)).month] for d in range(RANGE_DIAS)]
_day_indices = list(range(RANGE_DIAS))


def _data_sazonal() -> int:
    return rng.choices(_day_indices, weights=_day_weights, k=1)[0]


# ── 1. fornecedores.csv ─────────────────────────────────────────────
print(f"Gerando fornecedores.csv  ({N_FORNECEDORES:,} linhas)...")
with open(HERE / "fornecedores.csv", "w", newline="", encoding="utf-8") as f:
    w = csv.writer(f)
    w.writerow(["fornecedor_id", "razao_social", "cnpj", "regiao", "cidade",
                "contato", "segmento"])
    for i in range(1, N_FORNECEDORES + 1):
        regiao = rng.choice(REGIOES)
        w.writerow([
            i,
            fake.company(),
            fake.cnpj(),
            regiao,
            rng.choice(CIDADES[regiao]),
            fake.phone_number(),
            rng.choice(SEGMENTOS),
        ])


# ── 2. clientes.csv ──────────────────────────────────────────────────
print(f"Gerando clientes.csv  ({N_CLIENTES:,} linhas)...")
cliente_regiao = {}
with open(HERE / "clientes.csv", "w", newline="", encoding="utf-8", buffering=1 << 20) as f:
    w = csv.writer(f)
    w.writerow(["cliente_id", "nome", "tipo_cliente", "documento", "regiao",
                "cidade", "data_cadastro", "ativo", "score_credito"])
    BATCH = 10_000
    rows = []
    for i in range(1, N_CLIENTES + 1):
        regiao = rng.choice(REGIOES)
        tipo = rng.choices(["PF", "PJ"], weights=[85, 15])[0]
        nome = fake.name() if tipo == "PF" else fake.company()
        doc  = fake.cpf() if tipo == "PF" else fake.cnpj()
        cliente_regiao[i] = regiao
        rows.append([
            i, nome, tipo, doc, regiao,
            rng.choice(CIDADES[regiao]),
            _data(rng.randint(0, RANGE_DIAS)),
            rng.choices([1, 0], weights=[85, 15])[0],
            round(rng.uniform(300, 1000), 1),
        ])
        if len(rows) == BATCH:
            w.writerows(rows)
            rows.clear()
    if rows:
        w.writerows(rows)


# ── 3. cupons.csv ───────────────────────────────────────────────────
print(f"Gerando cupons.csv  ({N_CUPONS:,} linhas)...")
cupons = []
with open(HERE / "cupons.csv", "w", newline="", encoding="utf-8") as f:
    w = csv.writer(f)
    w.writerow(["cupom_id", "codigo", "tipo", "valor", "validade_inicio",
                "validade_fim", "categoria", "uso_minimo"])
    for i in range(1, N_CUPONS + 1):
        tipo_cupom = rng.choice(["fixo", "percentual"])
        cat = rng.choice(CATEGORIAS + ["TODAS"])
        ini = rng.randint(0, RANGE_DIAS - 180)
        fim = min(ini + rng.randint(30, 180), RANGE_DIAS)
        uso_minimo = round(rng.uniform(50, 300), 2) if rng.random() > 0.3 else 0.0
        valor = round(rng.uniform(10, 100), 2) if tipo_cupom == "fixo" else rng.randint(5, 25)
        codigo = f"{fake.word().upper()[:4]}{rng.randint(1000, 9999)}"
        cupons.append({"id": i, "ini": ini, "fim": fim})
        w.writerow([i, codigo, tipo_cupom, valor, _data(ini), _data(fim), cat, uso_minimo])


# ── 4. produtos.csv ──────────────────────────────────────────────────
print(f"Gerando produtos.csv  ({N_PRODUTOS:,} linhas)...")
produto_preco = {}
with open(HERE / "produtos.csv", "w", newline="", encoding="utf-8") as f:
    w = csv.writer(f)
    w.writerow(["produto_id", "nome", "categoria", "preco", "estoque",
                "peso_kg", "fornecedor_id"])
    for i in range(1, N_PRODUTOS + 1):
        cat = rng.choice(CATEGORIAS)
        prefixo = rng.choice(PREFIXOS[cat])
        modelo = rng.randint(100, 999)
        lo, hi = PRECO_RANGES[cat]
        preco = round(rng.uniform(lo, hi), 2)
        produto_preco[i] = preco
        w.writerow([
            i, f"{prefixo} {modelo}", cat, preco,
            rng.randint(0, 500), round(rng.uniform(0.1, 30), 2),
            rng.randint(1, N_FORNECEDORES),
        ])


# ── 5. vendas.csv + fretes.csv + pagamentos.csv (mesmo loop) ────────
print(f"Gerando vendas.csv, fretes.csv, pagamentos.csv  ({N_VENDAS:,} linhas)...")

COMENTARIOS = {
    "positivo": [
        "Excelente produto!", "Entrega rápida e produto de qualidade.",
        "Recomendo!", "Muito bom, chegou antes do prazo.",
        "Produto conforme descrito.", "Satisfeito com a compra.",
        "Boa qualidade pelo preço.", "Entrega antes do previsto.",
    ],
    "neutro": [
        "Ok, mas poderia ser melhor.", "Produto razoável.",
        "Atendeu, mas demorou um pouco.", "Esperava mais pelo preço.",
        "Funciona bem, nada especial.", "Entrega dentro do prazo.",
    ],
    "negativo": [
        "Produto veio com defeito.", "Demora excessiva na entrega.",
        "Não recomendo.", "Péssima qualidade.", "Arrependi da compra.",
        "Produto diferente da descrição.", "Veio amassado.",
    ],
}
NOTAS = [5, 5, 5, 4, 4, 4, 3, 3, 2, 1]

with \
     open(HERE / "vendas.csv", "w", newline="", encoding="utf-8", buffering=4 << 20) as vf, \
     open(HERE / "fretes.csv", "w", newline="", encoding="utf-8", buffering=4 << 20) as ff, \
     open(HERE / "pagamentos.csv", "w", newline="", encoding="utf-8", buffering=4 << 20) as pf:
    w_v, w_f, w_p = csv.writer(vf), csv.writer(ff), csv.writer(pf)

    w_v.writerow(["venda_id", "cliente_id", "produto_id", "quantidade", "desconto_pct",
                  "cupom_id", "status", "canal", "data_venda", "hora_venda"])
    w_f.writerow(["frete_id", "venda_id", "valor_frete", "prazo_dias",
                  "transportadora", "status_entrega"])
    w_p.writerow(["pagamento_id", "venda_id", "metodo", "parcelas",
                  "valor_pago", "status_pagamento"])

    BATCH = 10_000
    vr, fr, pr = [], [], []
    concluidos = []  # (venda_id, dia_offset)

    for vid in range(1, N_VENDAS + 1):
        dia = _data_sazonal()
        data_v = _data(dia)
        hora = f"{rng.randint(6,22):02d}:{rng.randint(0,59):02d}:{rng.randint(0,59):02d}"
        cid = rng.randint(1, N_CLIENTES)
        pid = rng.randint(1, N_PRODUTOS)
        qty = rng.choices(range(1, 11), weights=[40,25,15,7,4,3,2,1.5,1,0.5])[0]
        status = rng.choice(STATUS)

        # Cupom: 20% das vendas usam cupom
        cupom_id = ""
        if rng.random() < 0.20:
            validos = [c for c in cupons if c["ini"] <= dia <= c["fim"]]
            if validos:
                cupom_id = str(rng.choice(validos)["id"])

        desc  = rng.choice(DESCONTOS)
        canal = rng.choice(CANAIS)
        vr.append([vid, cid, pid, qty, desc, cupom_id, status, canal, data_v, hora])

        # Frete — valor baseado na regiao
        reg = cliente_regiao[cid]
        fb  = rng.uniform(12, 45)
        if reg in ("Norte", "Nordeste"):
            fb *= 1.5
        elif reg == "Centro-Oeste":
            fb *= 1.2
        fv    = round(min(fb, 120), 2)
        prazo = rng.randint(3, 15)
        if reg in ("Norte", "Nordeste"):
            prazo = rng.randint(7, 20)
        if status == "CONCLUIDO":
            se = "ENTREGUE"
            prazo = rng.randint(1, max(2, prazo - 3))
        elif status == "CANCELADO":
            se = "CANCELADO"
        else:
            se = rng.choice(["PREPARANDO", "EM_TRANSITO"])
        fr.append([vid, vid, fv, prazo, rng.choice(TRANSPORTADORAS), se])

        # Pagamento
        punit = produto_preco[pid]
        val   = round(punit * qty * (1 - desc / 100) + fv, 2)
        metodo = rng.choices(
            ["pix", "cartao_credito", "cartao_debito", "boleto"],
            weights=[40, 35, 10, 15],
        )[0]
        parc = rng.randint(1, 12) if metodo == "cartao_credito" else 1
        if status == "CONCLUIDO":
            sp = "PAGO"
        elif status == "CANCELADO":
            sp = rng.choice(["ESTORNADO", "CANCELADO"])
        else:
            sp = rng.choice(["PENDENTE", "PROCESSANDO"])
        pr.append([vid, vid, metodo, parc, val, sp])

        if status == "CONCLUIDO":
            concluidos.append((vid, dia))

        if len(vr) == BATCH:
            w_v.writerows(vr)
            w_f.writerows(fr)
            w_p.writerows(pr)
            vr.clear(); fr.clear(); pr.clear()

    if vr:
        w_v.writerows(vr)
        w_f.writerows(fr)
        w_p.writerows(pr)


# ── 6. avaliacoes.csv ────────────────────────────────────────────────
n_avals   = int(len(concluidos) * 0.7)
avaliados = sorted(rng.sample(concluidos, n_avals))
print(f"Gerando avaliacoes.csv  ({n_avals:,} linhas)...")

with open(HERE / "avaliacoes.csv", "w", newline="", encoding="utf-8", buffering=4 << 20) as f:
    w = csv.writer(f)
    w.writerow(["avaliacao_id", "venda_id", "nota", "comentario", "data"])
    BATCH = 10_000
    rows = []
    for idx, (vid, dia) in enumerate(avaliados, 1):
        nota = rng.choice(NOTAS)
        if nota >= 4:
            tipo_c = "positivo"
        elif nota == 3:
            tipo_c = "neutro"
        else:
            tipo_c = "negativo"
        comentario = rng.choice(COMENTARIOS[tipo_c])
        data_av = _data(min(dia + rng.randint(1, 30), RANGE_DIAS))
        rows.append([idx, vid, nota, comentario, data_av])
        if len(rows) == BATCH:
            w.writerows(rows)
            rows.clear()
    if rows:
        w.writerows(rows)


print("\nDatasets gerados:")
for csv_file in sorted(HERE.glob("*.csv")):
    size_mb = csv_file.stat().st_size / 1_048_576
    print(f"  {csv_file.name:<20} {size_mb:>8.1f} MB")
