#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# v1.2 — mantém v1.1 e adiciona: varredura em raw/{receitas,empenhadas,liquidadas,pagas},
# roteamento por subpastas, coerção mínima por tabela destino (Entidade/Líquido),
# compat extra para 'ano'/'exercicio' e 'líquido' vs 'liquido'.

import os, sys, re, json, traceback, unicodedata
from pathlib import Path
from datetime import datetime
import argparse
import pandas as pd
import numpy as np
import psycopg2, psycopg2.extras as pgx

# ---------- CLI ----------
def parse_args():
    p = argparse.ArgumentParser(description="Carrega CSVs do Equiplano/Anexo10 para tabelas de staging.")
    p.add_argument("--csv", required=True, help="Pasta raiz com receitas/, empenhadas/, liquidadas/, pagas/")
    p.add_argument("--schema", default="public")
    p.add_argument("--verbose", action="store_true")
    p.add_argument("--dedupe", action="store_true", help="Remove linhas com a palavra 'total'")
    p.add_argument("--add-year", action="store_true", help="Cria/normaliza coluna 'ano' a partir de 'exercicio' ou do nome do arquivo")
    p.add_argument("--numeric", action="store_true",
                   help="Limpa colunas monetárias (remove milhares e troca vírgula por ponto) — mantém como texto na staging")
    return p.parse_args()

# ---------- Conexão ----------
def get_conn():
    url = os.getenv("DATABASE_URL")
    if not url:
        print("❌ Defina DATABASE_URL", file=sys.stderr); sys.exit(1)
    if url.startswith("postgresql+psycopg2://"):
        url = url.replace("postgresql+psycopg2://","postgresql://",1)
    return psycopg2.connect(url)

# ---------- Utils ----------
TOTAL_PAT = re.compile(r"(?i)\btotal\b")
DATE_IN_NAME = re.compile(r"(\d{4}-\d{2}-\d{2})")
YEAR_IN_PATH = re.compile(r"(\d{4})")
MONEY_LIKE = re.compile(r'^\s?-?\d{1,3}(\.\d{3})*,\d{2}\s?$')  # 1.234.567,89
LOG_DIR = Path("logs"); LOG_DIR.mkdir(exist_ok=True)

def strip_total_rows(df):
    if df.empty: return df
    mask = pd.Series(False, index=df.index)
    for c in df.columns:
        if pd.api.types.is_string_dtype(df[c]):
            mask |= df[c].fillna("").str.contains(TOTAL_PAT)
    return df.loc[~mask].copy()

def infer_year(path: Path):
    m = DATE_IN_NAME.search(path.name)
    if m:
        try: return datetime.strptime(m.group(1), "%Y-%m-%d").year
        except: pass
    m = YEAR_IN_PATH.search(path.as_posix())
    if m:
        y = int(m.group(1))
        if 1900 <= y <= 2100: return y
    return None

def slugify(s: str) -> str:
    # só para comparação, não uso para renomear (mantenho acentos nos nomes)
    return ''.join(ch for ch in unicodedata.normalize('NFKD', s) if not unicodedata.combining(ch))

def log_error(rel: Path, err: Exception, ctx: dict):
    p = LOG_DIR / ("__".join(rel.parts) + ".log")
    p.parent.mkdir(parents=True, exist_ok=True)
    with open(p, "w", encoding="utf-8") as fh:
        fh.write(json.dumps({
            "file": rel.as_posix(),
            "error": str(err),
            "traceback": traceback.format_exc(),
            "context": ctx
        }, ensure_ascii=False, indent=2))

# ---------- DB helpers ----------
def existing_cols(conn, schema, table):
    with conn.cursor() as cur:
        cur.execute("""
          SELECT column_name FROM information_schema.columns
           WHERE table_schema=%s AND table_name=%s ORDER BY ordinal_position
        """, (schema, table))
        return [r[0] for r in cur.fetchall()]

def ensure_table_and_cols(conn, schema, table, cols, verbose=False):
    cols = [str(c) for c in cols]
    cur_cols = existing_cols(conn, schema, table)
    if not cur_cols:
        with conn.cursor() as cur:
            cur.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}";')
            coldef = ", ".join(f'"{c}" TEXT' for c in cols)
            cur.execute(f'CREATE TABLE IF NOT EXISTS "{schema}"."{table}" ({coldef});')
        conn.commit()
        if verbose: print(f"🗃️ criada {schema}.{table} com {len(cols)} colunas")
        return
    # adiciona colunas novas se aparecerem
    missing = [c for c in cols if c not in cur_cols]
    if missing:
        with conn.cursor() as cur:
            for c in missing:
                cur.execute(f'ALTER TABLE "{schema}"."{table}" ADD COLUMN IF NOT EXISTS "{c}" TEXT;')
        conn.commit()
        if verbose: print(f"🧱 {schema}.{table}: +{len(missing)} colunas: {missing}")

def insert_df(conn, schema, table, df):
    if df.empty: return 0
    cols = list(df.columns)
    records = []
    for row in df.itertuples(index=False, name=None):
        rec = []
        for v in row:
            if pd.isna(v) or v == "":
                rec.append(None)
            else:
                rec.append(str(v))
        records.append(tuple(rec))
    with conn.cursor() as cur:
        pgx.execute_values(
            cur,
            f'INSERT INTO "{schema}"."{table}" ({",".join(f"""\"{c}\"""" for c in cols)}) VALUES %s',
            records, page_size=1000
        )
    conn.commit()
    return len(records)

# ---------- sniff de separador/encoding ----------
def sniff_sep_and_encoding(path: Path):
    # tenta utf-8-sig; se falhar, latin-1
    encodings = ["utf-8-sig", "latin-1", "utf-8"]
    with open(path, "rb") as fh:
        raw = fh.read(4096)
    sample = None; encoding = None
    for enc in encodings:
        try:
            sample = raw.decode(enc)
            encoding = enc
            break
        except UnicodeDecodeError:
            continue
    if sample is None:
        encoding = "utf-8-sig"
        sample = raw.decode("utf-8", errors="ignore")

    # conta ; e , para decidir
    semi = sample.count(";")
    comma = sample.count(",")
    sep = ";" if semi >= comma else ","
    return sep, encoding

# ---------- limpeza monetária ----------
def looks_money_series(s: pd.Series) -> bool:
    s2 = s.dropna().astype(str)
    if s2.empty: return False
    return (s2.str.match(MONEY_LIKE, na=False)).mean() >= 0.60

def money_ptbr_to_dot(v: str) -> str:
    if v is None: return v
    t = str(v).strip()
    if t == "" or t == "-": return ""
    if MONEY_LIKE.match(t):
        return t.replace(".", "").replace(",", ".")
    return v

def normalize_money_columns(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty: return df
    for c in df.columns:
        if pd.api.types.is_string_dtype(df[c]) and looks_money_series(df[c]):
            df[c] = df[c].apply(money_ptbr_to_dot)
    return df

# ---------- CSV parsing ----------
def load_csv(path: Path, add_year, dedupe, numeric, verbose):
    sep, enc = sniff_sep_and_encoding(path)
    if verbose:
        print(f"   → sep='{sep}' encoding='{enc}'")

    df = pd.read_csv(path, sep=sep, dtype=str, engine="python", encoding=enc)
    df = df.fillna("")

    # remove linhas 'total' (se houver)
    if dedupe:
        before = len(df); df = strip_total_rows(df)
        if verbose and before != len(df): print(f"   linhas 'total' removidas: {before - len(df)}")

    # normaliza nomes: minúsculas e troca espaço/hífen/“ – ” por underscore
    def norm_col(c: str) -> str:
        c2 = c.strip().replace("\ufeff","")
        c2 = c2.replace("–","-").replace("—","-")
        c2 = c2.lower().replace(" ", "_").replace("-", "_").replace("/", "_")
        c2 = re.sub(r"_+", "_", c2)
        return c2

    df.columns = [norm_col(c) for c in df.columns]

    # padroniza 'exercício' → 'exercicio'
    if "exercício" in df.columns and "exercicio" not in df.columns:
        df = df.rename(columns={"exercício":"exercicio"})

    # cria/normaliza 'ano'
    if add_year:
        if "ano" in df.columns:
            df["ano"] = df["ano"].astype(str).str.extract(r"(\d{4})")
        elif "exercicio" in df.columns:
            df["ano"] = df["exercicio"].astype(str).str.extract(r"(\d{4})")
        else:
            y = infer_year(path)
            if y: df["ano"] = str(y)

    # normalização monetária opcional (continua TEXT na staging)
    if numeric:
        df = normalize_money_columns(df)

    if verbose:
        print(f"   colunas finais: {list(df.columns)}")
        try: print("   preview:\n" + df.head(3).to_string(index=False))
        except: pass
    return df

# ---------- roteamento por pasta ----------
def route_table(root: Path, file_path: Path) -> str:
    rel = file_path.relative_to(root)
    top = rel.parts[0].lower()

    if top in ("receitas",):
        return "stg_receitas"     # anexo10_prev_arrec_YYYY.csv

    if top in ("empenhadas", "despesas_empenhadas"):
        return "stg_despesas_empenhadas"

    if top in ("liquidadas", "despesas_liquidadas"):
        return "stg_despesas_liquidadas"

    if top in ("pagas", "despesas_pagas"):
        return "stg_despesas_pagas"

    return "stg_outros"

# ---------- coerção mínima por destino (garante o “formato esperado”) ----------
def _first_present(df: pd.DataFrame, candidates: list[str]) -> str | None:
    for c in candidates:
        if c in df.columns:
            return c
    return None

def coerce_for_table(df: pd.DataFrame, target: str, verbose=False) -> pd.DataFrame:
    # RECEITAS (Anexo 10)
    if target == "stg_receitas":
        # nomes usuais: ano|exercicio, codigo, especificacao, subitem, previsao, arrecadacao, para_mais, para_menos
        # aceita variações mínimas
        if "exercicio" not in df.columns and "ano" in df.columns:
            df = df.rename(columns={"ano": "exercicio"})
        # garante colunas (se não tiver, cria vazia)
        needed = ["exercicio","codigo","especificacao","subitem","previsao","arrecadacao","para_mais","para_menos"]
        for c in needed:
            if c not in df.columns:
                df[c] = ""
        # ordena
        df = df[needed]
        return df

    # DESPESAS (Equiplano): total vem em "Líquido" nos três relatórios
    if target in ("stg_despesas_empenhadas","stg_despesas_liquidadas","stg_despesas_pagas"):
        # nomes normalizados já estão minúsculos; pode haver acento: 'líquido'
        liquido_col = _first_present(df, ["líquido", "liquido"])
        if liquidity := liquido_col:
            pass
        else:
            # às vezes vem com variações tipo 'liquido_total'
            liquido_col = _first_present(df, ["liquido_total","líquido_total","liquido___total"])
        if liquido_col is None:
            if verbose:
                print("   ⚠️ coluna de total ('líquido'/'liquido') não encontrada — mantendo como está")
            return df

        # 'Entidade' já vira 'entidade' na normalização de cabeçalhos
        if "entidade" not in df.columns:
            df["entidade"] = ""

        # 'exercicio' pode vir como 'ano'
        if "exercicio" not in df.columns and "ano" in df.columns:
            df = df.rename(columns={"ano":"exercicio"})

        # reordena para trio mínimo
        base = ["exercicio","entidade",liquido_col]
        for c in base:
            if c not in df.columns:
                df[c] = ""
        df = df[base]
        # padroniza o nome da coluna total para 'liquido' (sem acento) na staging
        df = df.rename(columns={liquido_col: "liquido"})
        return df

    return df

# ---------- main ----------
def main():
    args = parse_args()
    root = Path(args.csv).resolve()
    if not root.exists():
        print(f"❌ pasta não existe: {root}", file=sys.stderr); sys.exit(1)

    try:
        conn = get_conn()
    except Exception as e:
        print("❌ falha na conexão Postgres:", e, file=sys.stderr); sys.exit(2)

    files = sorted(root.rglob("*.csv"))
    print(f"⚙️  encontrados {len(files)} CSVs sob {root}")
    total = 0

    for f in files:
        rel = f.relative_to(root)
        target = route_table(root, f)
        print(f"\n⚙️  Carregando {rel} → {args.schema}.{target}")
        try:
            df = load_csv(f, args.add_year, args.dedupe, args.numeric, args.verbose)
            df = coerce_for_table(df, target, verbose=args.verbose)

            if df.empty:
                print("   (vazio) — ignorado."); continue

            ensure_table_and_cols(conn, args.schema, target, list(df.columns), verbose=args.verbose)
            n = insert_df(conn, args.schema, target, df)
            total += n
            print(f"✅ Inserido: {n} linha(s).")
        except Exception as e:
            try: conn.rollback()
            except: pass
            ctx = {"columns": list(df.columns) if 'df' in locals() else None}
            log_error(rel, e, ctx)
            print(f"❌ Erro ao processar {rel}: {e}")
            print(f"   → veja o log em: logs/{'__'.join(rel.parts)}.log")

    print(f"\n🎯 Concluído. Linhas inseridas: {total}")

if __name__ == "__main__":
    main()