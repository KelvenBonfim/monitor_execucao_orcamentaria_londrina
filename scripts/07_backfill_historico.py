#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
07_backfill_historico.py — Recalcula e repovoa os Fatos a partir da Staging.

Fluxo:
  1) Valida a existência das tabelas de staging necessárias.
  2) Resolve dinamicamente nomes de colunas (ano, entidade, especificação e valores).
  3) Deleta dos Fatos os anos-alvo (--years).
  4) (Despesa) Agrega Empenhado, Liquidado (Orçamento + RAP) e Pago (Orçamento + RAP) por (exercicio, entidade).
  5) (Receita) Agrega Previsão e Arrecadação por (exercicio, especificacao).
  6) Insere nos Fatos.
  7) (Opcional) VACUUM/ANALYZE (VACUUM fora de transação/autocommit).

Uso:
  export DATABASE_URL="postgresql://user:pass@host:5432/db"
  python scripts/07_backfill_historico.py \
    --schema public \
    --staging public \
    --years 2018-2025 \
    --vacuum \
    --analyze \
    --verbose

Observações:
- Script transacional para os passos de DELETE/INSERT (rollback em falhas).
- Conversão numérica robusta (pt-BR/US; negativos entre parênteses) aplicada no SQL.
- Falha explícita se faltar coluna essencial (ano/entidade/especificação/valores).
"""

import argparse
import os
import sys
import unicodedata
from typing import Dict, List, Optional

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Connection

# ========================= Utils / Infra =========================

def norm_txt(s: str) -> str:
    if s is None:
        return ""
    s = s.strip().lower()
    s = "".join(c for c in unicodedata.normalize("NFD", s) if unicodedata.category(c) != "Mn")
    return s

def norm_key(s: str) -> str:
    s = norm_txt(s)
    return "".join(ch if ch.isalnum() else "_" for ch in s)

def parse_years(arg: Optional[str]) -> List[int]:
    if not arg:
        raise SystemExit("❌ Informe --years (ex.: 2018-2025 ou 2018,2019)")
    s = arg.strip()
    years: List[int] = []
    if "-" in s:
        a, b = s.split("-", 1)
        years = list(range(int(a), int(b) + 1))
    else:
        for x in s.replace(",", " ").split():
            years.append(int(x))
    if not years:
        raise SystemExit("❌ Formato inválido para --years")
    return years

def eng_from_env():
    url = os.getenv("DATABASE_URL")
    if not url:
        print("❌ Defina DATABASE_URL", file=sys.stderr)
        sys.exit(1)
    # aceitar variações +psycopg2
    url = url.replace("postgresql+psycopg2://", "postgresql://")
    return create_engine(url, future=True)

def to_numeric_sql(col_q: str) -> str:
    """
    Conversor SQL robusto pt-BR/US:
      - vírgula => trata como pt-BR (remove pontos, vírgula->ponto)
      - parênteses => negativo  (123,45) -> -123.45
      - caracteres não numéricos removidos
    """
    return f"""
    (
      CASE
        WHEN {col_q} IS NULL OR {col_q} = '' THEN 0::numeric
        ELSE
          (CASE WHEN {col_q} LIKE '(%%' AND {col_q} LIKE '%%)' THEN -1 ELSE 1 END) *
          (
            CASE
              WHEN {col_q} LIKE '%%,%%' THEN
                NULLIF(
                  REPLACE(
                    REPLACE(
                      REGEXP_REPLACE({col_q}, '[^0-9,().-]', '', 'g'),
                    '.', ''), ',', '.'
                  ),
                '')::numeric
              ELSE
                NULLIF(
                  REGEXP_REPLACE({col_q}, '[^0-9().-]', '', 'g'),
                '')::numeric
            END
          )
      END
    )
    """

def get_columns(con, schema: str, table: str) -> List[str]:
    sql = """
      SELECT column_name
      FROM information_schema.columns
      WHERE table_schema = :s AND table_name = :t
      ORDER BY ordinal_position;
    """
    rows = con.execute(text(sql), {"s": schema, "t": table}).fetchall()
    return [r[0] for r in rows]

def find_col_exact_or_prefix(cols: List[str], candidates: List[str]) -> Optional[str]:
    cmap = {norm_txt(c): c for c in cols}
    # exato
    for cand in candidates:
        k = norm_txt(cand)
        if k in cmap:
            return cmap[k]
    # prefixo
    for cand in candidates:
        k = norm_txt(cand)
        for nk, v in cmap.items():
            if nk.startswith(k):
                return v
    return None

def find_col_contains(cols: List[str], terms: List[str]) -> Optional[str]:
    nmap = {c: norm_key(c) for c in cols}
    for col, nk in nmap.items():
        if all(t in nk for t in terms):
            return col
    return None

# ========================= Resolution (staging) =========================

def resolve_year_col(con, schema: str, table: str) -> str:
    cols = get_columns(con, schema, table)
    y = find_col_exact_or_prefix(cols, ["exercicio", "ano"])
    if not y:
        raise RuntimeError(f"Coluna de ano não encontrada em {schema}.{table}. Colunas: {cols}")
    return y

def resolve_entidade_col(con, schema: str, table: str) -> str:
    cols = get_columns(con, schema, table)
    cand = find_col_exact_or_prefix(cols, ["entidade", "orgão", "orgao", "unidade_orcamentaria", "unidade orcamentaria", "unidade"])
    if not cand:
        raise RuntimeError(f"Coluna de entidade não encontrada em {schema}.{table}. Colunas: {cols}")
    return cand

def resolve_receita_cols(con, schema: str) -> Dict[str, str]:
    cols = get_columns(con, schema, "stg_receitas")
    y = find_col_exact_or_prefix(cols, ["exercicio", "ano"])
    if not y:
        raise RuntimeError(f"Coluna de ano não encontrada em {schema}.stg_receitas. Colunas: {cols}")

    espec = find_col_exact_or_prefix(cols, ["especificacao", "especificação", "descricao", "descrição"])
    if not espec:
        raise RuntimeError(f"Coluna de especificação não encontrada em {schema}.stg_receitas. Colunas: {cols}")

    prev = find_col_exact_or_prefix(cols, ["previsao", "previsão"])
    arr  = find_col_exact_or_prefix(cols, ["arrecadacao", "arrecadação"])

    if not prev or not arr:
        raise RuntimeError(f"Colunas de valores (previsao/arrecadacao) não encontradas em {schema}.stg_receitas. Colunas: {cols}")

    return {"year": y, "espec": espec, "prev": prev, "arr": arr}

def resolve_despesa_value_cols(con, schema: str) -> Dict[str, Optional[str]]:
    """
    Resolve colunas de valor nas 3 tabelas de despesa:
      - stg_despesas_empenhadas:     líquido (fallback: empenhado)
      - stg_despesas_liquidadas:     líquido_orçamento + líquido_restos
      - stg_despesas_pagas:          pago_orçamento + pago_restos
    """
    out = {"emp": None, "emp_fallback": None, "liq_orc": None, "liq_rap": None, "pag_orc": None, "pag_rap": None}

    cols_emp = get_columns(con, schema, "stg_despesas_empenhadas")
    out["emp"] = find_col_contains(cols_emp, ["liquido"]) or None
    if not out["emp"]:
        out["emp_fallback"] = find_col_contains(cols_emp, ["empenhad"])

    cols_liq = get_columns(con, schema, "stg_despesas_liquidadas")
    out["liq_orc"] = find_col_contains(cols_liq, ["liquid", "orcamento"]) or find_col_contains(cols_liq, ["liquido", "orcamento"])
    out["liq_rap"] = (find_col_contains(cols_liq, ["liquid", "restos"]) or
                      find_col_contains(cols_liq, ["liquid", "pagar"]) or
                      find_col_contains(cols_liq, ["liquido", "restos"]) or
                      find_col_contains(cols_liq, ["liquido", "pagar"]))

    cols_pag = get_columns(con, schema, "stg_despesas_pagas")
    out["pag_orc"] = find_col_contains(cols_pag, ["pago", "orcamento"]) or find_col_contains(cols_pag, ["pago", "orc"])
    out["pag_rap"] = find_col_contains(cols_pag, ["pago", "restos"]) or find_col_contains(cols_pag, ["pago", "pagar"])

    return out

# ========================= Backfill SQL builders =========================

def sql_build_backfill_despesa(schema_f: str, schema_s: str,
                               y_emp: str, y_liq: str, y_pag: str,
                               entidade_emp: str, entidade_liq: str, entidade_pag: str,
                               vcols: Dict[str, Optional[str]],
                               years: List[int]) -> str:
    emp_expr = to_numeric_sql(f'"{vcols["emp"]}"') if vcols["emp"] else (
               to_numeric_sql(f'"{vcols["emp_fallback"]}"') if vcols["emp_fallback"] else "0::numeric")
    liq_orc_expr = to_numeric_sql(f'"{vcols["liq_orc"]}"') if vcols["liq_orc"] else "0::numeric"
    liq_rap_expr = to_numeric_sql(f'"{vcols["liq_rap"]}"') if vcols["liq_rap"] else "0::numeric"
    pag_orc_expr = to_numeric_sql(f'"{vcols["pag_orc"]}"') if vcols["pag_orc"] else "0::numeric"
    pag_rap_expr = to_numeric_sql(f'"{vcols["pag_rap"]}"') if vcols["pag_rap"] else "0::numeric"

    years_sql = "(" + ",".join(str(y) for y in years) + ")"

    return f"""
    -- Apaga anos-alvo em fato_despesa
    DELETE FROM "{schema_f}"."fato_despesa" WHERE exercicio IN {years_sql};

    WITH emp AS (
      SELECT NULLIF(e."{y_emp}", '')::int AS exercicio,
             e."{entidade_emp}"::text      AS entidade,
             SUM(COALESCE({emp_expr},0))   AS v_emp
      FROM "{schema_s}"."stg_despesas_empenhadas" e
      WHERE NULLIF(e."{y_emp}", '')::int IN {years_sql}
      GROUP BY 1,2
    ),
    liq AS (
      SELECT NULLIF(l."{y_liq}", '')::int AS exercicio,
             l."{entidade_liq}"::text      AS entidade,
             SUM(COALESCE({liq_orc_expr},0) + COALESCE({liq_rap_expr},0)) AS v_liq
      FROM "{schema_s}"."stg_despesas_liquidadas" l
      WHERE NULLIF(l."{y_liq}", '')::int IN {years_sql}
      GROUP BY 1,2
    ),
    pag AS (
      SELECT NULLIF(p."{y_pag}", '')::int AS exercicio,
             p."{entidade_pag}"::text      AS entidade,
             SUM(COALESCE({pag_orc_expr},0) + COALESCE({pag_rap_expr},0)) AS v_pag
      FROM "{schema_s}"."stg_despesas_pagas" p
      WHERE NULLIF(p."{y_pag}", '')::int IN {years_sql}
      GROUP BY 1,2
    ),
    j AS (
      SELECT COALESCE(emp.exercicio, liq.exercicio, pag.exercicio) AS exercicio,
             COALESCE(emp.entidade,  liq.entidade,  pag.entidade ) AS entidade,
             emp.v_emp, liq.v_liq, pag.v_pag
      FROM emp
      FULL JOIN liq ON liq.exercicio = emp.exercicio AND liq.entidade = emp.entidade
      FULL JOIN pag ON pag.exercicio = COALESCE(emp.exercicio, liq.exercicio)
                   AND pag.entidade  = COALESCE(emp.entidade,  liq.entidade)
    )
    INSERT INTO "{schema_f}"."fato_despesa"(exercicio, entidade, valor_empenhado, valor_liquidado, valor_pago)
    SELECT j.exercicio, j.entidade,
           COALESCE(j.v_emp,0), COALESCE(j.v_liq,0), COALESCE(j.v_pag,0)
    FROM j;
    """

def sql_build_backfill_receita(schema_f: str, schema_s: str,
                               y_rec: str, espec: str, prev: str, arr: str,
                               years: List[int]) -> str:
    years_sql = "(" + ",".join(str(y) for y in years) + ")"
    prev_expr = to_numeric_sql(f'"{prev}"')
    arr_expr  = to_numeric_sql(f'"{arr}"')

    return f"""
    -- Apaga anos-alvo em fato_receita
    DELETE FROM "{schema_f}"."fato_receita" WHERE exercicio IN {years_sql};

    INSERT INTO "{schema_f}"."fato_receita"(exercicio, especificacao, previsao, arrecadacao)
    SELECT NULLIF(r."{y_rec}", '')::int AS exercicio,
           r."{espec}"::text            AS especificacao,
           SUM(COALESCE({prev_expr},0)) AS previsao,
           SUM(COALESCE({arr_expr},0))  AS arrecadacao
    FROM "{schema_s}"."stg_receitas" r
    WHERE NULLIF(r."{y_rec}", '')::int IN {years_sql}
    GROUP BY 1,2;
    """

# ========================= Maintenance (VACUUM/ANALYZE) =========================

def run_maintenance(engine, schema: str, do_vacuum: bool, do_analyze: bool, verbose: bool = False):
    """
    Executa VACUUM/ANALYZE corretamente:
      - VACUUM: precisa de AUTOCOMMIT (fora de transação)
      - ANALYZE: pode rodar em transação, mas usamos o mesmo canal autocommit
    """
    if not (do_vacuum or do_analyze):
        return

    with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as con:
        if do_vacuum:
            con.execute(text(f'VACUUM (VERBOSE) "{schema}"."fato_despesa";'))
            con.execute(text(f'VACUUM (VERBOSE) "{schema}"."fato_receita";'))
            if verbose:
                print("🧹 VACUUM executado (autocommit).")
        if do_analyze:
            con.execute(text(f'ANALYZE "{schema}"."fato_despesa";'))
            con.execute(text(f'ANALYZE "{schema}"."fato_receita";'))
            if verbose:
                print("📊 ANALYZE executado.")

# ========================= Main =========================

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--schema", default="public", help="Esquema dos Fatos (destino)")
    ap.add_argument("--staging", default="public", help="Esquema da Staging (origem)")
    ap.add_argument("--years", required=True, help="Faixa/lista: 2018-2025 ou 2018,2019")
    ap.add_argument("--vacuum", action="store_true", help="Executa VACUUM nas tabelas de fatos ao final")
    ap.add_argument("--analyze", action="store_true", help="Executa ANALYZE nas tabelas de fatos ao final")
    ap.add_argument("--verbose", action="store_true")
    args = ap.parse_args()

    years = parse_years(args.years)
    engine = eng_from_env()

    with engine.begin() as con:
        if args.verbose:
            print(f"➡️  Backfill anos {years} (fatos em {args.schema}, staging em {args.staging})")

        # Validação básica de staging
        required = [
            "stg_despesas_empenhadas",
            "stg_despesas_liquidadas",
            "stg_despesas_pagas",
            "stg_receitas",
        ]
        for t in required:
            con.execute(text(f"""
              DO $$
              BEGIN
                IF NOT EXISTS (
                  SELECT 1 FROM information_schema.tables
                  WHERE table_schema = :s AND table_name = :t
                ) THEN
                  RAISE EXCEPTION 'Tabela % não encontrada em %.%', :t, :s, :t;
                END IF;
              END $$;
            """), {"s": args.staging, "t": t})

        # Resolver colunas
        y_emp = resolve_year_col(con, args.staging, "stg_despesas_empenhadas")
        y_liq = resolve_year_col(con, args.staging, "stg_despesas_liquidadas")
        y_pag = resolve_year_col(con, args.staging, "stg_despesas_pagas")

        entidade_emp = resolve_entidade_col(con, args.staging, "stg_despesas_empenhadas")
        entidade_liq = resolve_entidade_col(con, args.staging, "stg_despesas_liquidadas")
        entidade_pag = resolve_entidade_col(con, args.staging, "stg_despesas_pagas")

        vcols = resolve_despesa_value_cols(con, args.staging)
        if args.verbose:
            print(f"   • Despesa: ano(emp/li/pag) = {y_emp}/{y_liq}/{y_pag}")
            print(f"   • Despesa: entidade(emp/li/pag) = {entidade_emp}/{entidade_liq}/{entidade_pag}")
            print(f"   • Despesa: valores => emp={vcols['emp'] or vcols['emp_fallback']} | liq_orc={vcols['liq_orc']} | liq_rap={vcols['liq_rap']} | pag_orc={vcols['pag_orc']} | pag_rap={vcols['pag_rap']}")

        y_rec_cols = resolve_receita_cols(con, args.staging)
        if args.verbose:
            print(f"   • Receita: ano={y_rec_cols['year']} | espec={y_rec_cols['espec']} | prev={y_rec_cols['prev']} | arr={y_rec_cols['arr']}")

        # Executa backfill de DESPESA
        sql_desp = sql_build_backfill_despesa(
            schema_f=args.schema,
            schema_s=args.staging,
            y_emp=y_emp, y_liq=y_liq, y_pag=y_pag,
            entidade_emp=entidade_emp, entidade_liq=entidade_liq, entidade_pag=entidade_pag,
            vcols=vcols,
            years=years
        )
        con.execute(text(sql_desp))
        if args.verbose:
            print("✅ Fato Despesa backfilled.")

        # Executa backfill de RECEITA
        sql_rec = sql_build_backfill_receita(
            schema_f=args.schema,
            schema_s=args.staging,
            y_rec=y_rec_cols["year"], espec=y_rec_cols["espec"],
            prev=y_rec_cols["prev"], arr=y_rec_cols["arr"],
            years=years
        )
        con.execute(text(sql_rec))
        if args.verbose:
            print("✅ Fato Receita backfilled.")

    # Manutenção (fora da transação)
    run_maintenance(engine, args.schema, args.vacuum, args.analyze, args.verbose)

    if args.verbose:
        print("🏁 Backfill concluído com sucesso.")

if __name__ == "__main__":
    main()
