#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
05_build_models.py
Cria/atualiza os modelos consolidados:
  - fato_despesa(exercicio, entidade, valor_empenhado, valor_liquidado, valor_pago)
  - fato_receita(exercicio, codigo, especificacao, previsao, arrecadacao)

Lê das tabelas de staging (carregadas pelo 04_load_csv_to_postgres.py):
  - stg_despesas_empenhadas  (colunas chave: exercicio/ano, entidade, empenhado)
  - stg_despesas_liquidadas  (exercicio/ano, entidade, liquidado_-_orçamento, liquidado_-_restos_a_pagar)
  - stg_despesas_pagas       (exercicio/ano, entidade, pago_-_orçamento, pago_-_restos_a_pagar)
  - stg_receitas             (exercicio/ano, codigo, especificacao, previsao, arrecadacao)

Auto-descobre colunas mesmo com acentos/hífens/underscores, converte textos pt-BR/US p/ numeric
e agrega.

Uso:
  export DATABASE_URL="postgresql://user:pass@host:5432/db"   # use 'postgresql://' (sem +psycopg2)
  python scripts/05_build_models.py \
    --schema public \
    --staging public \
    --years 2018-2025 \
    --recreate \
    --verbose
"""

import argparse
import os
import sys
import unicodedata
from typing import List, Optional, Tuple

import psycopg2
import psycopg2.extras as pgx


# ------------------------
# CLI
# ------------------------
def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--schema", default="public", help="Esquema destino (fatos)")
    p.add_argument("--staging", default="public", help="Esquema de staging (origem)")
    p.add_argument("--years", help="Faixa ou lista: 2018-2025 ou 2018,2019")
    p.add_argument("--recreate", action="store_true", help="Drop + create as tabelas de fatos")
    p.add_argument("--verbose", action="store_true")
    return p.parse_args()


# ------------------------
# Conexão
# ------------------------
def get_conn():
    url = os.getenv("DATABASE_URL")
    if not url:
        print("❌ Defina DATABASE_URL", file=sys.stderr)
        sys.exit(1)
    if url.startswith("postgresql+psycopg2://"):
        url = url.replace("postgresql+psycopg2://", "postgresql://", 1)
    return psycopg2.connect(url)


# ------------------------
# Helpers
# ------------------------
def norm(s: str) -> str:
    """normaliza p/ matching: lower, sem acento, remove espaços/_/-"""
    if s is None:
        return ""
    s = s.strip().lower()
    s = "".join(c for c in unicodedata.normalize("NFD", s) if unicodedata.category(c) != "Mn")
    s = s.replace(" ", "").replace("_", "").replace("-", "")
    return s

def parse_years(arg: Optional[str]) -> Optional[List[int]]:
    if not arg:
        return None
    s = arg.strip()
    if "-" in s:
        a, b = s.split("-", 1)
        return list(range(int(a), int(b) + 1))
    out = []
    for x in s.replace(",", " ").split():
        try:
            out.append(int(x))
        except:
            pass
    return out or None

def fetch_columns(cur, schema: str, table: str) -> List[str]:
    cur.execute("""
        SELECT column_name
          FROM information_schema.columns
         WHERE table_schema=%s AND table_name=%s
         ORDER BY ordinal_position
    """, (schema, table))
    return [r[0] for r in cur.fetchall()]

def find_col(cols: List[str], candidates: List[str]) -> Optional[str]:
    """
    Tenta achar uma coluna entre 'cols' que case com qq um dos 'candidates' (normalizados).
    """
    cmap = {norm(c): c for c in cols}
    for cand in candidates:
        nc = norm(cand)
        if nc in cmap:
            return cmap[nc]
    # tenta 'startswith' normalizado
    for cand in candidates:
        nc = norm(cand)
        for k, v in cmap.items():
            if k.startswith(nc):
                return v
    return None

def ensure_schema(cur, schema: str):
    cur.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}";')

def recreate_table(cur, schema: str, name: str, ddl_cols: str):
    cur.execute(f'DROP TABLE IF EXISTS "{schema}"."{name}" CASCADE;')
    cur.execute(f'CREATE TABLE "{schema}"."{name}" ({ddl_cols});')

def ensure_indexes(cur, schema: str, table: str, cols: List[str]):
    for c in cols:
        idx = f"{table}_{c}_idx".lower()
        cur.execute(f'CREATE INDEX IF NOT EXISTS "{idx}" ON "{schema}"."{table}" ("{c}");')

def to_numeric_sql(col_quoted: str) -> str:
    """
    Converte:
      - pt-BR:  1.234.567,89  (vírgula presente)  -> tira pontos, troca vírgula por ponto
      - US:     1234567.89    (sem vírgula)       -> mantém ponto como decimal
    Também trata negativos com parênteses.
    """
    return f"""
    (
      CASE
        WHEN {col_quoted} IS NULL OR {col_quoted} = '' THEN 0::numeric
        ELSE
          (
            CASE WHEN {col_quoted} LIKE '(%%' AND {col_quoted} LIKE '%%)' THEN -1 ELSE 1 END
          ) * (
            CASE
              WHEN {col_quoted} LIKE '%%,%%' THEN
                NULLIF(
                  REPLACE(
                    REPLACE(
                      REGEXP_REPLACE({col_quoted}, '[^0-9,().-]', '', 'g'),
                    '.', ''), ',', '.'
                  ),
                '')::numeric
              ELSE
                NULLIF(
                  REGEXP_REPLACE({col_quoted}, '[^0-9().-]', '', 'g'),
                '')::numeric
            END
          )
      END
    )
    """

def verbose_stage_totals(cur, schema_stg: str, years: Optional[List[int]]):
    """Imprime totais por ano direto do STAGING (para auditoria), detectando colunas dinamicamente."""

    def run_total(table: str, year_col: str, expr_sql: str):
        where = f'WHERE NULLIF("{year_col}", \'\')::int = ANY(%s)' if years else ''
        params = (years,) if years else None
        cur.execute(f'''
          SELECT NULLIF("{year_col}", '')::int AS ano,
                 {expr_sql} AS total
          FROM "{schema_stg}"."{table}"
          {where}
          GROUP BY 1
          ORDER BY 1;
        ''', params)
        return cur.fetchall()

    # --- Empenhadas ---
    cols_emp = fetch_columns(cur, schema_stg, "stg_despesas_empenhadas")
    y_emp = find_col(cols_emp, ["exercicio", "ano"]) or "exercicio"
    c_emp = find_col(cols_emp, ["empenhado"]) or "empenhado"
    emp_expr = f"SUM({to_numeric_sql(f'\"{c_emp}\"')})"
    emp = run_total("stg_despesas_empenhadas", y_emp, emp_expr)
    print("STG empenhadas:", emp)

    # --- Liquidadas (orcamento + RAP) ---
    cols_liq = fetch_columns(cur, schema_stg, "stg_despesas_liquidadas")
    y_liq = find_col(cols_liq, ["exercicio", "ano"]) or "exercicio"
    liq_orc = find_col(cols_liq, [
        "liquidado_-_orçamento","liquidado_orçamento","liquidadoorcamento",
        "liquidado - orçamento","liquidado_orcamento"
    ])
    liq_rap = find_col(cols_liq, [
        "liquidado_-_restos_a_pagar","liquidado_restos_a_pagar","liquidadorestosapagar",
        "liquidado - restos a pagar"
    ])
    liq_orc_sql = to_numeric_sql(f'"{liq_orc}"') if liq_orc else "0::numeric"
    liq_rap_sql = to_numeric_sql(f'"{liq_rap}"') if liq_rap else "0::numeric"
    liq_expr = f"SUM(COALESCE({liq_orc_sql},0) + COALESCE({liq_rap_sql},0))"
    liq = run_total("stg_despesas_liquidadas", y_liq, liq_expr)
    print("STG liquidadas:", liq)

    # --- Pagas (orcamento + RAP) ---
    cols_pag = fetch_columns(cur, schema_stg, "stg_despesas_pagas")
    y_pag = find_col(cols_pag, ["exercicio", "ano"]) or "exercicio"
    pag_orc = find_col(cols_pag, [
        "pago_-_orçamento","pago_orçamento","pagoorcamento",
        "pago - orçamento","pago_orcamento"
    ])
    pag_rap = find_col(cols_pag, [
        "pago_-_restos_a_pagar","pago_restos_a_pagar","pagorestosapagar",
        "pago - restos a pagar"
    ])
    pag_orc_sql = to_numeric_sql(f'"{pag_orc}"') if pag_orc else "0::numeric"
    pag_rap_sql = to_numeric_sql(f'"{pag_rap}"') if pag_rap else "0::numeric"
    pag_expr = f"SUM(COALESCE({pag_orc_sql},0) + COALESCE({pag_rap_sql},0))"
    pag = run_total("stg_despesas_pagas", y_pag, pag_expr)
    print("STG pagas:", pag)

    # --- Receitas (previsão + arrecadação, só como referência de volume) ---
    cols_rec = fetch_columns(cur, schema_stg, "stg_receitas")
    y_rec = find_col(cols_rec, ["exercicio", "ano"]) or "ano"
    c_prev = find_col(cols_rec, ["previsao"]) or "previsao"
    c_arr  = find_col(cols_rec, ["arrecadacao"]) or "arrecadacao"
    rec_expr = f"SUM(COALESCE({to_numeric_sql(f'\"{c_prev}\"')},0) + COALESCE({to_numeric_sql(f'\"{c_arr}\"')},0))"
    rec = run_total("stg_receitas", y_rec, rec_expr)
    print("STG receitas (prev+arr):", rec)

# ------------------------
# Build FATO RECEITA
# ------------------------
def build_fato_receita(cur, schema_dst: str, schema_stg: str, years: Optional[List[int]], verbose: bool):
    table_stg = "stg_receitas"
    cols = fetch_columns(cur, schema_stg, table_stg)
    if not cols:
        print(f"⚠️  {schema_stg}.{table_stg} não encontrada; pulando receitas.")
        return

    c_exercicio = find_col(cols, ["exercicio", "ano"])
    c_codigo    = find_col(cols, ["codigo"])
    c_espec     = find_col(cols, ["especificacao"])
    c_prev      = find_col(cols, ["previsao"])
    c_arr       = find_col(cols, ["arrecadacao"])

    for need, nm in [(c_exercicio, "exercicio/ano"), (c_codigo, "codigo"),
                     (c_espec, "especificacao"), (c_prev, "previsao"), (c_arr, "arrecadacao")]:
        if need is None:
            print(f"❌ staging {table_stg}: coluna obrigatória ausente ({nm})", file=sys.stderr)
            sys.exit(2)

    # (re)create destino
    recreate_table(cur, schema_dst, "fato_receita", '''
        exercicio     int,
        codigo        text,
        especificacao text,
        previsao      numeric(20,2),
        arrecadacao   numeric(20,2)
    ''')

    where_parts = []
    params: Tuple = ()
    # filtra linhas TOTAL (defensivo — evita dupla contagem)
    where_parts.append(f"NOT (UPPER(COALESCE(\"{c_codigo}\", '')) = 'TOTAL' OR UPPER(COALESCE(\"{c_espec}\", '')) = 'TOTAL')")
    if years:
        where_parts.append(f'NULLIF("{c_exercicio}", \'\')::int = ANY(%s)')
        params = (years,)
    where_sql = ("WHERE " + " AND ".join(where_parts)) if where_parts else ""

    sql = f'''
    INSERT INTO "{schema_dst}"."fato_receita"(exercicio, codigo, especificacao, previsao, arrecadacao)
    SELECT
      NULLIF("{c_exercicio}", '')::int  AS exercicio,
      "{c_codigo}"::text                AS codigo,
      "{c_espec}"::text                 AS especificacao,
      COALESCE({to_numeric_sql(f'"{c_prev}"')}, 0) AS previsao,
      COALESCE({to_numeric_sql(f'"{c_arr}"')}, 0)  AS arrecadacao
    FROM "{schema_stg}"."{table_stg}"
    {where_sql};
    '''
    if verbose:
        print("→ Montando fato_receita…")
    cur.execute(sql, params if years else None)
    ensure_indexes(cur, schema_dst, "fato_receita", ["exercicio", "codigo"])


# ------------------------
# Build FATO DESPESA
# ------------------------
def build_fato_despesa(cur, schema_dst: str, schema_stg: str, years: Optional[List[int]], verbose: bool):
    stg_emp = "stg_despesas_empenhadas"
    stg_liq = "stg_despesas_liquidadas"
    stg_pag = "stg_despesas_pagas"

    cols_emp = fetch_columns(cur, schema_stg, stg_emp)
    cols_liq = fetch_columns(cur, schema_stg, stg_liq)
    cols_pag = fetch_columns(cur, schema_stg, stg_pag)
    if not (cols_emp and cols_liq and cols_pag):
        print(f"⚠️  staging de despesas ausente (emp/liquid/pagas); pulando despesas.")
        return

    emp_ex  = find_col(cols_emp, ["exercicio", "ano"])
    emp_ent = find_col(cols_emp, ["entidade"])
    emp_val = find_col(cols_emp, ["empenhado"])
    if not (emp_ex and emp_ent and emp_val):
        print("❌ stg_despesas_empenhadas: não achei exercicio/entidade/empenhado", file=sys.stderr); sys.exit(2)

    liq_ex  = find_col(cols_liq, ["exercicio", "ano"])
    liq_ent = find_col(cols_liq, ["entidade"])
    liq_orc = find_col(cols_liq, ["liquidado_-_orçamento", "liquidadoorcamento", "liquidado - orçamento", "liquidado_orcamento"])
    liq_rap = find_col(cols_liq, ["liquidado_-_restos_a_pagar", "liquidadorestosapagar", "liquidado - restos a pagar", "liquidado_restos_a_pagar"])
    if not (liq_ex and liq_ent and (liq_orc or liq_rap)):
        print("❌ stg_despesas_liquidadas: colunas mínimas não encontradas", file=sys.stderr); sys.exit(2)

    pag_ex  = find_col(cols_pag, ["exercicio", "ano"])
    pag_ent = find_col(cols_pag, ["entidade"])
    pag_orc = find_col(cols_pag, ["pago_-_orçamento", "pagoorcamento", "pago - orçamento", "pago_orcamento"])
    pag_rap = find_col(cols_pag, ["pago_-_restos_a_pagar", "pagorestosapagar", "pago - restos a pagar", "pago_restos_a_pagar"])
    if not (pag_ex and pag_ent and (pag_orc or pag_rap)):
        print("❌ stg_despesas_pagas: colunas mínimas não encontradas", file=sys.stderr); sys.exit(2)

    recreate_table(cur, schema_dst, "fato_despesa", '''
        exercicio       int,
        entidade        text,
        valor_empenhado numeric(20,2),
        valor_liquidado numeric(20,2),
        valor_pago      numeric(20,2)
    ''')

    where_emp = ""
    where_liq = ""
    where_pag = ""
    params: Tuple = ()
    if years:
        where_emp = f'WHERE NULLIF("{emp_ex}", \'\')::int = ANY(%s)'
        where_liq = f'WHERE NULLIF("{liq_ex}", \'\')::int = ANY(%s)'
        where_pag = f'WHERE NULLIF("{pag_ex}", \'\')::int = ANY(%s)'
        params = (years, years, years)

    sql = f'''
    WITH emp AS (
      SELECT
        NULLIF("{emp_ex}", '')::int  AS exercicio,
        "{emp_ent}"::text            AS entidade,
        COALESCE({to_numeric_sql(f'"{emp_val}"')}, 0) AS valor_empenhado
      FROM "{schema_stg}"."{stg_emp}"
      {where_emp}
    ),
    liq AS (
      SELECT
        NULLIF("{liq_ex}", '')::int  AS exercicio,
        "{liq_ent}"::text            AS entidade,
        COALESCE({to_numeric_sql(f'"{liq_orc}"') if liq_orc else "0::numeric"}, 0)
        + COALESCE({to_numeric_sql(f'"{liq_rap}"') if liq_rap else "0::numeric"}, 0)
        AS valor_liquidado
      FROM "{schema_stg}"."{stg_liq}"
      {where_liq}
    ),
    pag AS (
      SELECT
        NULLIF("{pag_ex}", '')::int  AS exercicio,
        "{pag_ent}"::text            AS entidade,
        COALESCE({to_numeric_sql(f'"{pag_orc}"') if pag_orc else "0::numeric"}, 0)
        + COALESCE({to_numeric_sql(f'"{pag_rap}"') if pag_rap else "0::numeric"}, 0)
        AS valor_pago
      FROM "{schema_stg}"."{stg_pag}"
      {where_pag}
    ),
    agg_emp AS (
      SELECT exercicio, entidade, SUM(valor_empenhado) AS valor_empenhado
      FROM emp GROUP BY exercicio, entidade
    ),
    agg_liq AS (
      SELECT exercicio, entidade, SUM(valor_liquidado) AS valor_liquidado
      FROM liq GROUP BY exercicio, entidade
    ),
    agg_pag AS (
      SELECT exercicio, entidade, SUM(valor_pago) AS valor_pago
      FROM pag GROUP BY exercicio, entidade
    )
    INSERT INTO "{schema_dst}"."fato_despesa"(exercicio, entidade, valor_empenhado, valor_liquidado, valor_pago)
    SELECT
      COALESCE(e.exercicio, l.exercicio, p.exercicio) AS exercicio,
      COALESCE(e.entidade,  l.entidade,  p.entidade ) AS entidade,
      COALESCE(e.valor_empenhado, 0) AS valor_empenhado,
      COALESCE(l.valor_liquidado, 0) AS valor_liquidado,
      COALESCE(p.valor_pago, 0)      AS valor_pago
    FROM agg_emp e
    FULL OUTER JOIN agg_liq l
      ON l.exercicio = e.exercicio AND l.entidade = e.entidade
    FULL OUTER JOIN agg_pag p
      ON p.exercicio = COALESCE(e.exercicio, l.exercicio)
     AND p.entidade  = COALESCE(e.entidade,  l.entidade)
    ;
    '''
    if verbose:
        print("→ Montando fato_despesa…")
    cur.execute(sql, params if years else None)
    ensure_indexes(cur, schema_dst, "fato_despesa", ["exercicio", "entidade"])


# ------------------------
# MAIN
# ------------------------
def main():
    args = parse_args()
    years = parse_years(args.years)

    with get_conn() as conn:
        with conn.cursor() as cur:
            ensure_schema(cur, args.schema)
            if args.recreate and args.verbose:
                print("🧹 recriando tabelas de fatos…")

            if args.verbose:
                # auditoria: totais direto do STAGING
                verbose_stage_totals(cur, args.staging, years)

            # fato_receita
            build_fato_receita(cur, args.schema, args.staging, years, args.verbose)
            # fato_despesa
            build_fato_despesa(cur, args.schema, args.staging, years, args.verbose)

        conn.commit()

    print("✅ modelos criados/atualizados com sucesso.")

if __name__ == "__main__":
    main()
