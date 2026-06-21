"""Gera CSVs de exemplo com volume realista para testes Spark."""
import csv
import random
from datetime import datetime, timedelta
from pathlib import Path

from faker import Faker

HERE = Path(__file__).parent

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
RANGE_DIAS = 900

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

MONTH_WEIGHTS = {
    1: 5, 2: 7, 3: 9, 4: 8, 5: 10, 6: 7,
    7: 7, 8: 8, 9: 8, 10: 9, 11: 15, 12: 15,
}

DESCONTOS        = [0, 0, 0, 5, 5, 10, 15, 20]
TRANSPORTADORAS  = ["Correios", "Jadlog", "Total Express", "Braspress"]
SEGMENTOS        = ["Eletrônicos", "Alimentos", "Têxtil", "Cosméticos", "Livros",
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


# ── Helpers ─────────────────────────────────────────────────────────
def _data(offset_dias: int) -> str:
    return (BASE_DATE + timedelta(days=offset_dias)).strftime("%Y-%m-%d")


_day_weights = [MONTH_WEIGHTS[(BASE_DATE + timedelta(days=d)).month] for d in range(RANGE_DIAS)]
_day_indices = list(range(RANGE_DIAS))


def _make_rng(seed: int = 42) -> random.Random:
    return random.Random(seed)


def _make_fake(seed: int = 42) -> Faker:
    f = Faker("pt_BR")
    f.seed_instance(seed)
    return f


# ── Funções de geração ───────────────────────────────────────────────
def gerar_fornecedores(output_dir: str | Path | None = None,
                       run_timestamp: str | None = None) -> str:
    if run_timestamp is None:
        run_timestamp = datetime.now().strftime("%Y%m%dT%H%M%S")
    out = Path(output_dir) if output_dir else HERE
    rng = _make_rng(42)
    fake = _make_fake(42)

    print(f"Gerando fornecedores_{run_timestamp}.csv  ({N_FORNECEDORES:,} linhas)...")
    path = out / f"fornecedores_{run_timestamp}.csv"
    with open(path, "w", newline="", encoding="utf-8") as f:
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
    print(f"  -> {path} ({path.stat().st_size / 1_048_576:.1f} MB)")
    return str(path)


def gerar_clientes(output_dir: str | Path | None = None,
                   run_timestamp: str | None = None) -> str:
    if run_timestamp is None:
        run_timestamp = datetime.now().strftime("%Y%m%dT%H%M%S")
    out = Path(output_dir) if output_dir else HERE
    rng = _make_rng(43)
    fake = _make_fake(43)

    print(f"Gerando clientes_{run_timestamp}.csv  ({N_CLIENTES:,} linhas)...")
    path = out / f"clientes_{run_timestamp}.csv"
    with open(path, "w", newline="", encoding="utf-8", buffering=1 << 20) as f:
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
    print(f"  -> {path} ({path.stat().st_size / 1_048_576:.1f} MB)")
    return str(path)


def gerar_cupons(output_dir: str | Path | None = None,
                 run_timestamp: str | None = None) -> str:
    if run_timestamp is None:
        run_timestamp = datetime.now().strftime("%Y%m%dT%H%M%S")
    out = Path(output_dir) if output_dir else HERE
    rng = _make_rng(44)
    fake = _make_fake(44)

    print(f"Gerando cupons_{run_timestamp}.csv  ({N_CUPONS:,} linhas)...")
    path = out / f"cupons_{run_timestamp}.csv"
    with open(path, "w", newline="", encoding="utf-8") as f:
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
            w.writerow([i, codigo, tipo_cupom, valor, _data(ini), _data(fim), cat, uso_minimo])
    print(f"  -> {path} ({path.stat().st_size / 1_048_576:.1f} MB)")
    return str(path)


def gerar_produtos(output_dir: str | Path | None = None,
                   run_timestamp: str | None = None) -> str:
    if run_timestamp is None:
        run_timestamp = datetime.now().strftime("%Y%m%dT%H%M%S")
    out = Path(output_dir) if output_dir else HERE
    rng = _make_rng(45)

    print(f"Gerando produtos_{run_timestamp}.csv  ({N_PRODUTOS:,} linhas)...")
    path = out / f"produtos_{run_timestamp}.csv"
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["produto_id", "nome", "categoria", "preco", "estoque",
                     "peso_kg", "fornecedor_id"])
        for i in range(1, N_PRODUTOS + 1):
            cat = rng.choice(CATEGORIAS)
            prefixo = rng.choice(PREFIXOS[cat])
            modelo = rng.randint(100, 999)
            lo, hi = PRECO_RANGES[cat]
            preco = round(rng.uniform(lo, hi), 2)
            w.writerow([
                i, f"{prefixo} {modelo}", cat, preco,
                rng.randint(0, 500), round(rng.uniform(0.1, 30), 2),
                rng.randint(1, N_FORNECEDORES),
            ])
    print(f"  -> {path} ({path.stat().st_size / 1_048_576:.1f} MB)")
    return str(path)


def gerar_vendas_fretes_pagamentos(output_dir: str | Path | None = None,
                                   run_timestamp: str | None = None) -> str:
    if run_timestamp is None:
        run_timestamp = datetime.now().strftime("%Y%m%dT%H%M%S")
    out = Path(output_dir) if output_dir else HERE
    rng = _make_rng(46)

    # Reconstroi lookups a partir dos CSVs ja gerados (mesmo run_timestamp)
    cliente_regiao = {}
    with open(out / f"clientes_{run_timestamp}.csv", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            cliente_regiao[int(row["cliente_id"])] = row["regiao"]

    produto_preco = {}
    with open(out / f"produtos_{run_timestamp}.csv", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            produto_preco[int(row["produto_id"])] = float(row["preco"])

    cupons = []
    with open(out / f"cupons_{run_timestamp}.csv", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            ini = (datetime.strptime(row["validade_inicio"], "%Y-%m-%d") - BASE_DATE).days
            fim = (datetime.strptime(row["validade_fim"], "%Y-%m-%d") - BASE_DATE).days
            cupons.append({"id": int(row["cupom_id"]), "ini": ini, "fim": fim})

    ts = run_timestamp
    print(f"Gerando vendas_{ts}.csv, fretes_{ts}.csv, pagamentos_{ts}.csv  ({N_VENDAS:,} linhas)...")
    concluidos = []

    with \
         open(out / f"vendas_{ts}.csv", "w", newline="", encoding="utf-8", buffering=4 << 20) as vf, \
         open(out / f"fretes_{ts}.csv", "w", newline="", encoding="utf-8", buffering=4 << 20) as ff, \
         open(out / f"pagamentos_{ts}.csv", "w", newline="", encoding="utf-8", buffering=4 << 20) as pf:
        w_v, w_f, w_p = csv.writer(vf), csv.writer(ff), csv.writer(pf)

        w_v.writerow(["venda_id", "cliente_id", "produto_id", "quantidade", "desconto_pct",
                      "cupom_id", "status", "canal", "data_venda", "hora_venda"])
        w_f.writerow(["frete_id", "venda_id", "valor_frete", "prazo_dias",
                      "transportadora", "status_entrega"])
        w_p.writerow(["pagamento_id", "venda_id", "metodo", "parcelas",
                      "valor_pago", "status_pagamento"])

        BATCH = 10_000
        vr, fr, pr = [], [], []

        for vid in range(1, N_VENDAS + 1):
            dia = rng.choices(_day_indices, weights=_day_weights, k=1)[0]
            data_v = _data(dia)
            hora = f"{rng.randint(6,22):02d}:{rng.randint(0,59):02d}:{rng.randint(0,59):02d}"
            cid = rng.randint(1, N_CLIENTES)
            pid = rng.randint(1, N_PRODUTOS)
            qty = rng.choices(range(1, 11), weights=[40,25,15,7,4,3,2,1.5,1,0.5])[0]
            status = rng.choice(STATUS)

            cupom_id = ""
            if rng.random() < 0.20:
                validos = [c for c in cupons if c["ini"] <= dia <= c["fim"]]
                if validos:
                    cupom_id = str(rng.choice(validos)["id"])

            desc  = rng.choice(DESCONTOS)
            canal = rng.choice(CANAIS)
            vr.append([vid, cid, pid, qty, desc, cupom_id, status, canal, data_v, hora])

            reg = cliente_regiao.get(cid, "Sudeste")
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

            punit = produto_preco.get(pid, 100.0)
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

    # Salva lista de concluidos para uso da task de avaliacoes
    concluidos_path = out / f"_concluidos_{ts}.txt"
    with open(concluidos_path, "w") as f:
        for vid, dia in concluidos:
            f.write(f"{vid},{dia}\n")

    for nome in ["vendas", "fretes", "pagamentos"]:
        p = out / f"{nome}_{ts}.csv"
        print(f"  -> {p} ({p.stat().st_size / 1_048_576:.1f} MB)")
    return str(out / f"vendas_{ts}.csv")


def gerar_avaliacoes(output_dir: str | Path | None = None,
                     run_timestamp: str | None = None) -> str:
    if run_timestamp is None:
        run_timestamp = datetime.now().strftime("%Y%m%dT%H%M%S")
    out = Path(output_dir) if output_dir else HERE
    rng = _make_rng(47)

    # Reconstroi lista de vendas concluidas
    concluidos = []
    with open(out / f"_concluidos_{run_timestamp}.txt", encoding="utf-8") as f:
        for line in f:
            vid, dia = line.strip().split(",")
            concluidos.append((int(vid), int(dia)))

    n_avals   = int(len(concluidos) * 0.7)
    avaliados = sorted(rng.sample(concluidos, n_avals))
    ts = run_timestamp
    print(f"Gerando avaliacoes_{ts}.csv  ({n_avals:,} linhas)...")

    path = out / f"avaliacoes_{ts}.csv"
    with open(path, "w", newline="", encoding="utf-8", buffering=4 << 20) as f:
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

    # Limpa arquivo temporario
    tmp = out / f"_concluidos_{ts}.txt"
    if tmp.exists():
        tmp.unlink()

    print(f"  -> {path} ({path.stat().st_size / 1_048_576:.1f} MB)")
    return str(path)


# ── Execução standalone ──────────────────────────────────────────────
if __name__ == "__main__":
    ts = datetime.now().strftime("%Y%m%dT%H%M%S")
    gerar_fornecedores(run_timestamp=ts)
    gerar_clientes(run_timestamp=ts)
    gerar_cupons(run_timestamp=ts)
    gerar_produtos(run_timestamp=ts)
    gerar_vendas_fretes_pagamentos(run_timestamp=ts)
    gerar_avaliacoes(run_timestamp=ts)

    print(f"\nDatasets gerados (run_timestamp={ts}):")
    for csv_file in sorted(HERE.glob(f"*_{ts}.csv")):
        size_mb = csv_file.stat().st_size / 1_048_576
        print(f"  {csv_file.name:<35} {size_mb:>8.1f} MB")
