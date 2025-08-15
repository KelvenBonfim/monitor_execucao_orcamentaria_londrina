#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
05_build_models.py
Carrega os CSVs do Anexo 10 (Receita Prevista x Arrecadada) para o Postgres e
constrói as views usadas pelo app.

Entrada esperada (padrão do projeto):
  raw/receitas/anexo10_prev_arrec_2018.csv
  raw/receitas/anexo10_prev_arrec_2019.csv
  ...
Esquema CSV:
  ano,codigo,especificacao,subitem,previsao,arrecadacao,para_mais,para_menos

Uso típico:
  export DATABASE_URL='postgresql://user:pass@host:5432/dbname'
  python scripts/05_build_models.py \
      --csvdir raw/receitas \
      --schema public \
      --staging public \
      --years 2018-2025 \
      --recreate \
      --verbose

Dependências: pandas, sqlalchemy, psycopg2-binary
"""

from __future__ import annotations
import argparse
import os
from pathlib import Path
from typing import List

import pandas as pd
from sqlalchemy import create_engine, text

# ---------------------------------------------------------------------

DDL_STG_RECEITAS = """
CREATE TABLE IF NOT EXISTS {staging_schema}.stg_receitas (
    ano           INTEGER NOT NULL,
    codigo        TEXT    NOT NULL,
    especificacao TEXT    NULL,
    subitem       TEXT    NULL,
    previsao      NUMERIC NULL,
    arrecadacao   NUMERIC NULL,
    para_mais     NUMERIC NULL,
    para_menos    NUMERIC NULL
);
"""

# Índices simples para acelerar GROUP BY/filters mais comuns
IDX_STG_RECEITAS = [
    "CREATE INDEX IF NOT EXISTS idx_stg_receitas_ano ON {staging_schema}.stg_receitas(ano);",
    "CREATE INDEX IF NOT EXISTS idx_stg_receitas_codigo ON {staging_schema}.stg_receitas(codigo);",
    "CREATE INDEX IF NOT EXISTS idx_stg_receitas_especificacao ON {staging_schema}.stg_receitas(especificacao);",
    "CREATE INDEX IF NOT EXISTS idx_stg_receitas_subitem ON {staging_schema}.stg_receitas(subitem);",
]

# Views (nivel categoria, subitens e resumo anual)
VW_RECEITA_POR_TIPO = """
CREATE OR REPLACE VIEW {schema}.vw_receita_por_tipo AS
SELECT
    ano,
    LPAD(codigo, 2, '0') AS codigo,   -- garante "11", "12", ...
    TRIM(especificacao)  AS especificacao,
    SUM(previsao)   AS previsao,
    SUM(arrecadacao) AS arrecadacao,
    SUM(para_mais)   AS para_mais,
    SUM(para_menos)  AS para_menos
FROM {staging_schema}.stg_receitas
WHERE COALESCE(NULLIF(TRIM(subitem), ''), NULL) IS NULL       -- ignora subitens
  AND COALESCE(NULLIF(TRIM(especificacao), ''), NULL) IS NOT NULL
GROUP BY ano, LPAD(codigo, 2, '0'), TRIM(especificacao)
ORDER BY ano, codigo;
"""

VW_RECEITA_POR_SUBITEM = """
CREATE OR REPLACE VIEW {schema}.vw_receita_por_subitem AS
SELECT
    ano,
    LPAD(codigo, 2, '0') AS codigo,
    TRIM(COALESCE(especificacao, '')) AS especificacao_pai,
    TRIM(subitem) AS subitem,
    SUM(previsao)   AS previsao,
    SUM(arrecadacao) AS arrecadacao,
    SUM(para_mais)   AS para_mais,
    SUM(para_menos)  AS para_menos
FROM {staging_schema}.stg_receitas
WHERE COALESCE(NULLIF(TRIM(subitem), ''), NULL) IS NOT NULL
GROUP BY ano, LPAD(codigo, 2, '0'), TRIM(COALESCE(especificacao, '')), TRIM(subitem)
ORDER BY ano, codigo, especificacao_pai, subitem;
"""

VW_RECEITA_RESUMO_ANUAL = """
CREATE OR REPLACE VIEW {schema}.vw_receita_resumo_anual AS
SELECT
    ano,
    SUM(previsao)   AS previsao_total,
    SUM(arrecadacao) AS arrecadacao_total,
    SUM(para_mais)   AS para_mais_total,
    SUM(para_menos)  AS para_menos_total
FROM {staging_schema}.stg_receitas
GROUP BY ano
ORDER BY ano;
"""

# ---------------------------------------------------------------------

def info(msg: str, verbose: bool):
    if verbose:
        print(msg)

def ensure_schema(conn, schema: str):
    conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema};"))

def recreate_staging(conn, staging_schema: str, verbose: bool):
    info("🧹 Recriando staging stg_receitas…", verbose)
    conn.execute(text(f"DROP TABLE IF EXISTS {staging_schema}.stg_receitas;"))
    conn.execute(text(DDL_STG_RECEITAS.format(staging_schema=staging_schema)))
    for ddl in IDX_STG_RECEITAS:
        conn.execute(text(ddl.format(staging_schema=staging_schema)))

def create_if_not_exists_staging(conn, staging_schema: str, verbose: bool):
    info("ℹ️  Garantindo staging stg_receitas…", verbose)
    conn.execute(text(DDL_STG_RECEITAS.format(staging_schema=staging_schema)))
    for ddl in IDX_STG_RECEITAS:
        conn.execute(text(ddl.format(staging_schema=staging_schema)))

def create_views(conn, schema: str, staging_schema: str, verbose: bool):
    info("📐 (Re)criando views…", verbose)
    conn.execute(text(VW_RECEITA_POR_TIPO.format(schema=schema, staging_schema=staging_schema)))
    conn.execute(text(VW_RECEITA_POR_SUBITEM.format(schema=schema, staging_schema=staging_schema)))
    conn.execute(text(VW_RECEITA_RESUMO_ANUAL.format(schema=schema, staging_schema=staging_schema)))

def load_csvs(engine, csvdir: Path, years: List[int], staging_schema: str, verbose: bool):
    dest_table = f"{staging_schema}.stg_receitas"
    for y in years:
        csv_path = csvdir / f"anexo10_prev_arrec_{y}.csv"
        if not csv_path.exists():
            info(f"⚠️  Arquivo não encontrado: {csv_path}", verbose)
            continue

        info(f"⬆️  Carregando {csv_path.name}…", verbose)
        df = pd.read_csv(csv_path)

        # Normalizações seguras
        expected_cols = ["ano","codigo","especificacao","subitem","previsao","arrecadacao","para_mais","para_menos"]
        missing = [c for c in expected_cols if c not in df.columns]
        if missing:
            raise ValueError(f"{csv_path.name}: colunas ausentes: {missing}")

        df["ano"] = df["ano"].astype(int)
        df["codigo"] = df["codigo"].astype(str).str.zfill(2)
        for c in ["especificacao","subitem"]:
            df[c] = df[c].fillna("").astype(str)

        # garante numéricos (NaN -> None)
        for c in ["previsao","arrecadacao","para_mais","para_menos"]:
            df[c] = pd.to_numeric(df[c], errors="coerce")

        # grava
        df.to_sql(
            name="stg_receitas",
            con=engine,
            schema=staging_schema,
            if_exists="append",
            index=False,
            method="multi",
            chunksize=10_000,
        )

# ---------------------------------------------------------------------

def parse_args():
    ap = argparse.ArgumentParser(description="Carrega receitas (Anexo 10) e cria views no Postgres.")
    ap.add_argument("--csvdir", required=True, help="Diretório com os CSVs anexo10_prev_arrec_<ANO>.csv (ex.: raw/receitas)")
    ap.add_argument("--schema", default="public", help="Schema alvo das views (default: public)")
    ap.add_argument("--staging", default="public", help="Schema de staging (default: public)")
    ap.add_argument("--years", required=True, help="Intervalo de anos, ex.: 2018-2025 ou lista: 2018,2019,2020")
    ap.add_argument("--recreate", action="store_true", help="Drop & create da tabela de staging")
    ap.add_argument("--verbose", action="store_true")
    return ap.parse_args()

def parse_years(arg: str) -> List[int]:
    arg = arg.strip()
    if "-" in arg:
        a, b = arg.split("-", 1)
        return list(range(int(a), int(b) + 1))
    return [int(x.strip()) for x in arg.split(",") if x.strip()]

# ---------------------------------------------------------------------

def main():
    args = parse_args()
    csvdir = Path(args.csvdir)
    years = parse_years(args.years)

    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        raise SystemExit("ERROR: defina a variável de ambiente DATABASE_URL.")

    engine = create_engine(db_url)
    with engine.begin() as conn:
        ensure_schema(conn, args.staging)
        ensure_schema(conn, args.schema)

        if args.recreate:
            recreate_staging(conn, args.staging, args.verbose)
        else:
            create_if_not_exists_staging(conn, args.staging, args.verbose)

    # carga
    load_csvs(engine, csvdir, years, args.staging, args.verbose)

    # views
    with engine.begin() as conn:
        create_views(conn, args.schema, args.staging, args.verbose)

    if args.verbose:
        print("✅ Finalizado com sucesso.")

if __name__ == "__main__":
    main()
