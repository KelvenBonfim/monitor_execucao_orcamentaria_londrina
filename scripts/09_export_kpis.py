#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
09_export_kpis.py
Exporta KPIs anuais para data/kpis/<ano>/ a partir do Postgres (local/Neon).

Exemplos:
  # Todos os anos detectados no banco
  python scripts/09_export_kpis.py --schema public --all-years --outdir data/kpis

  # Intervalo: 2018–2025
  python scripts/09_export_kpis.py --schema public --years 2018-2025 --outdir data/kpis --verbose

  # Lista: 2018,2019,2021
  python scripts/09_export_kpis.py --schema public --years 2018,2019,2021 --outdir data/kpis

  # Um ano específico
  python scripts/09_export_kpis.py --schema public --year 2024 --outdir data/kpis
"""

from __future__ import annotations
import argparse
import os
import sys
import json
from pathlib import Path
from typing import List

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine


# --------------------------
# Parsing de argumentos
# --------------------------
def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--schema", default="public", help="Schema do Postgres (ex.: public)")
    p.add_argument("--staging", default=None, help="Compatibilidade; não usado diretamente")
    p.add_argument("--year", type=int, help="Exportar um único ano")
    p.add_argument("--years", help="Intervalo 'YYYY-YYYY' ou lista 'YYYY,YYYY,...'")
    p.add_argument("--all-years", action="store_true", help="Exporta todos os anos encontrados")
    p.add_argument("--outdir", default="data/kpis", help="Diretório base de saída")
    p.add_argument("--top-n", type=int, default=15, help="Top N (efeito em alguns JSONs)")
    p.add_argument("--despesa-metrica",
                   choices=["pago", "liquidado", "empenhado"],
                   default="liquidado",
                   help="Métrica padrão de despesa")
    p.add_argument("--rawdir", default=None, help="Compat: caminho RAW (não usado)")
    p.add_argument("--qcdir", default=None, help="Compat: caminho QC (não usado)")
    p.add_argument("--verbose", action="store_true", help="Logs detalhados")
    return p.parse_args()


# --------------------------
# Utilitários
# --------------------------
def log(msg: str, verbose: bool = True):
    if verbose:
        print(msg, flush=True)


def ensure_dir(p: Path):
    p.mkdir(parents=True, exist_ok=True)


def get_engine() -> Engine:
    url = os.environ.get("DATABASE_URL")
    if not url:
        print("ERROR: defina DATABASE_URL no ambiente.", file=sys.stderr)
        sys.exit(1)
    return create_engine(url)


def parse_years_arg(years_arg: str) -> List[int]:
    """
    Aceita "2018-2025" ou "2018,2019,2021" ou "2024".
    """
    if not years_arg:
        return []
    s = years_arg.strip()
    if "-" in s:
        a, b = s.split("-", 1)
        ai, bi = int(a), int(b)
        if bi < ai:
            ai, bi = bi, ai
        return list(range(ai, bi + 1))
    if "," in s:
        return [int(x.strip()) for x in s.split(",") if x.strip()]
    if s.isdigit():
        return [int(s)]
    raise ValueError(f"Formato inválido para --years: {years_arg}")


def discover_all_years(engine: Engine, schema: str) -> List[int]:
    """
    Une anos de fato_despesa e fato_receita.
    """
    sql = text(f"""
        WITH y1 AS (
            SELECT DISTINCT exercicio::int AS ano
            FROM {schema}.fato_despesa
        ),
        y2 AS (
            SELECT DISTINCT exercicio::int AS ano
            FROM {schema}.fato_receita
        )
        SELECT DISTINCT ano FROM (
          SELECT ano FROM y1
          UNION
          SELECT ano FROM y2
        ) t
        ORDER BY ano;
    """)
    df = pd.read_sql(sql, engine)
    return df["ano"].astype(int).tolist()


def write_csv_and_json(df: pd.DataFrame, out_csv: Path, out_json: Path | None = None, json_preview_rows: int = 5):
    ensure_dir(out_csv.parent)
    df.to_csv(out_csv, index=False)
    if out_json:
        payload = {
            "rows": len(df),
            "cols": list(df.columns),
            "sample": df.head(json_preview_rows).to_dict(orient="records"),
        }
        out_json.write_text(json.dumps(payload, ensure_ascii=False, indent=2))


def safe_first_scalar(x):
    """
    Corrige FutureWarning: se vier Series(1), pega .iloc[0]; se vazio, None; se escalar, retorna direto.
    """
    if isinstance(x, pd.Series):
        if len(x) == 0:
            return None
        return x.iloc[0]
    return x


# --------------------------
# Descoberta de colunas
# --------------------------
def col_exists(engine: Engine, schema: str, table: str, col: str) -> bool:
    sql = text("""
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = :schema
          AND table_name   = :table
          AND lower(column_name) = lower(:col)
        LIMIT 1;
    """)
    with engine.connect() as conn:
        r = conn.execute(sql, {"schema": schema, "table": table, "col": col}).fetchone()
    return r is not None


def pick_first_existing(engine: Engine, schema: str, table: str, candidates: list[str]) -> str:
    for c in candidates:
        if col_exists(engine, schema, table, c):
            return c
    raise RuntimeError(
        f"Nenhuma das colunas {candidates} existe em {schema}.{table}. "
        "Ajuste os nomes das colunas candidatas para seu schema real."
    )


# -----------------------------------------
# SQL helpers (totais)
# -----------------------------------------
def sql_totais_despesa(schema: str) -> str:
    return f"""
      SELECT
        exercicio::int AS ano,
        SUM(valor_empenhado) AS empenhado,
        SUM(valor_liquidado) AS liquidado,
        SUM(valor_pago)      AS pago
      FROM {schema}.fato_despesa
      GROUP BY exercicio
      ORDER BY exercicio;
    """


def sql_totais_receita_v1(schema: str) -> str:
    return f"""
      SELECT
        exercicio::int AS ano,
        SUM(valor_previsto)   AS previsto,
        SUM(valor_arrecadado) AS arrecadado
      FROM {schema}.fato_receita
      GROUP BY exercicio
      ORDER BY exercicio;
    """


def sql_totais_receita_v2(schema: str) -> str:
    return f"""
      SELECT
        exercicio::int AS ano,
        SUM(previsao)    AS previsto,
        SUM(arrecadacao) AS arrecadado
      FROM {schema}.fato_receita
      GROUP BY exercicio
      ORDER BY exercicio;
    """


def load_totais_despesa(engine: Engine, schema: str) -> pd.DataFrame:
    return pd.read_sql(text(sql_totais_despesa(schema)), engine)


def load_totais_receita(engine: Engine, schema: str) -> pd.DataFrame:
    try:
        return pd.read_sql(text(sql_totais_receita_v1(schema)), engine)
    except Exception:
        return pd.read_sql(text(sql_totais_receita_v2(schema)), engine)


# --------------------------
# Exporters por KPI/ano
# --------------------------
def export_execucao_global_anual(engine: Engine, schema: str, outdir: Path, ano: int, verbose: bool):
    # CAST(:ano AS int) para evitar erro de bind param com ::
    sql = text(f"""
      SELECT
        CAST(:ano AS int) AS ano,
        SUM(CASE WHEN exercicio = :ano THEN valor_empenhado ELSE 0 END) AS empenhado,
        SUM(CASE WHEN exercicio = :ano THEN valor_liquidado ELSE 0 END) AS liquidado,
        SUM(CASE WHEN exercicio = :ano THEN valor_pago      ELSE 0 END) AS pago
      FROM {schema}.fato_despesa;
    """)
    df = pd.read_sql(sql, engine, params={"ano": int(ano)})
    out_csv = outdir / str(ano) / "execucao_global_anual.csv"
    out_json = outdir / str(ano) / "execucao_global_anual.json"
    write_csv_and_json(df, out_csv, out_json)
    log(f"📝 execucao_global_anual → {out_csv} | {out_json}", verbose)


def export_execucao_por_entidade_anual(engine: Engine, schema: str, outdir: Path, ano: int, verbose: bool):
    entidade_col = pick_first_existing(
        engine, schema, "fato_despesa",
        ["entidade", "nome_entidade", "entidade_nome", "descricao_entidade"]
    )
    if verbose:
        log(f"🔎 usando coluna de ENTIDADE: {entidade_col}", True)

    sql = text(f"""
      SELECT
        exercicio::int AS ano,
        {entidade_col} AS entidade,
        SUM(valor_empenhado) AS empenhado,
        SUM(valor_liquidado) AS liquidado,
        SUM(valor_pago)      AS pago
      FROM {schema}.fato_despesa
      WHERE exercicio = :ano
      GROUP BY 1,2
      ORDER BY pago DESC;
    """)
    df = pd.read_sql(sql, engine, params={"ano": int(ano)})
    out_csv = outdir / str(ano) / "execucao_por_entidade_anual.csv"
    out_json = outdir / str(ano) / "execucao_por_entidade_anual.json"
    write_csv_and_json(df, out_csv, out_json)
    log(f"📝 execucao_por_entidade_anual → {out_csv} | {out_json}", verbose)


def export_receita_prevista_arrecadada_anual(engine: Engine, schema: str, outdir: Path, ano: int, verbose: bool):
    df_all = load_totais_receita(engine, schema)
    row = df_all[df_all["ano"] == int(ano)].copy()
    if row.empty:
        row = pd.DataFrame([{"ano": int(ano), "previsto": 0.0, "arrecadado": 0.0}])
    row["gap"] = row["previsto"] - row["arrecadado"]
    for c in ["previsto", "arrecadado", "gap"]:
        row[c] = row[c].astype(float)
    out_csv = outdir / str(ano) / "receita_prevista_arrecadada_anual.csv"
    out_json = outdir / str(ano) / "receita_prevista_arrecadada_anual.json"
    write_csv_and_json(row, out_csv, out_json)
    log(f"📝 receita_prevista_arrecadada_anual → {out_csv} | {out_json}", verbose)


def export_superavit_deficit_anual(engine: Engine, schema: str, outdir: Path, ano: int, verbose: bool):
    d = load_totais_despesa(engine, schema)
    r = load_totais_receita(engine, schema)
    row_d = d[d["ano"] == int(ano)]
    row_r = r[r["ano"] == int(ano)]
    pago = float(safe_first_scalar(row_d["pago"])) if not row_d.empty else 0.0
    arrec = float(safe_first_scalar(row_r["arrecadado"])) if not row_r.empty else 0.0
    prev = float(safe_first_scalar(row_r["previsto"])) if not row_r.empty else 0.0

    out_val = {
        "ano": int(ano),
        "pago": pago,
        "arrecadado": arrec,
        "previsto": prev,
        "resultado": arrec - pago,  # + = superávit, - = déficit
        "diff_arrecadado_previsto": float(arrec - prev),
        "diff_previsto_arrecadado": float(prev - arrec),
    }

    df = pd.DataFrame([out_val])
    out_csv = outdir / str(ano) / "superavit_deficit_anual.csv"
    out_json = outdir / str(ano) / "superavit_deficit_anual.json"
    write_csv_and_json(df, out_csv, out_json)
    log(f"📝 superavit_deficit_anual → {out_csv} | {out_json}", verbose)


def export_validations_fatos_vs_staging(engine: Engine, schema: str, outdir: Path, ano: int, verbose: bool):
    d = load_totais_despesa(engine, schema)
    r = load_totais_receita(engine, schema)
    row_d = d[d["ano"] == int(ano)]
    row_r = r[r["ano"] == int(ano)]
    out = {
        "ano": int(ano),
        "despesa_empenhado": float(safe_first_scalar(row_d["empenhado"])) if not row_d.empty else 0.0,
        "despesa_liquidado": float(safe_first_scalar(row_d["liquidado"])) if not row_d.empty else 0.0,
        "despesa_pago": float(safe_first_scalar(row_d["pago"])) if not row_d.empty else 0.0,
        "receita_previsto": float(safe_first_scalar(row_r["previsto"])) if not row_r.empty else 0.0,
        "receita_arrecadado": float(safe_first_scalar(row_r["arrecadado"])) if not row_r.empty else 0.0,
    }
    df = pd.DataFrame([out])
    out_csv = outdir / str(ano) / "validations_fatos_vs_staging.csv"
    out_json = outdir / str(ano) / "validations_fatos_vs_staging.json"
    write_csv_and_json(df, out_csv, out_json)
    log(f"📝 validations_fatos_vs_staging → {out_csv} | {out_json}", verbose)


def export_data_coverage(engine: Engine, schema: str, outdir: Path, ano: int, verbose: bool):
    """Gera um JSON com estatísticas simples de cobertura para o ano (sem Decimals)."""
    cov = {"ano": int(ano)}
    with engine.connect() as conn:
        # Despesa
        qd = text(f"""
            SELECT
              COUNT(*)                                     AS linhas,
              COUNT(DISTINCT entidade)                     AS entidades,
              CAST(COALESCE(SUM(valor_empenhado), 0) AS double precision) AS empenhado,
              CAST(COALESCE(SUM(valor_liquidado), 0) AS double precision) AS liquidado,
              CAST(COALESCE(SUM(valor_pago),      0) AS double precision) AS pago
            FROM {schema}.fato_despesa
            WHERE exercicio = :ano
        """)
        rd = dict(conn.execute(qd, {"ano": int(ano)}).mappings().one())

        # Receita
        qr = text(f"""
            SELECT
              COUNT(*)                                        AS linhas,
              COUNT(DISTINCT codigo)                          AS codigos,
              CAST(COALESCE(SUM(previsao),    0) AS double precision) AS previsto,
              CAST(COALESCE(SUM(arrecadacao), 0) AS double precision) AS arrecadado
            FROM {schema}.fato_receita
            WHERE exercicio = :ano
        """)
        rr = dict(conn.execute(qr, {"ano": int(ano)}).mappings().one())

    # Força conversão para tipos builtin serializáveis
    def _to_builtin(d):
        out = {}
        for k, v in d.items():
            if v is None:
                out[k] = None
            elif isinstance(v, (int, float, str, bool)):
                out[k] = v
            else:
                try:
                    out[k] = float(v)
                except Exception:
                    out[k] = str(v)
        return out

    cov["despesa"] = _to_builtin(rd)
    cov["receita"] = _to_builtin(rr)

    out_json = outdir / str(ano) / "data_coverage_report.json"
    ensure_dir(out_json.parent)
    out_json.write_text(json.dumps(cov, ensure_ascii=False, indent=2))
    log(f"📄 data_coverage_report → {out_json}", verbose)


# ----- NOVOS KPIs com detecção dinâmica -----
def export_execucao_por_funcao_anual(engine: Engine, schema: str, outdir: Path, ano: int, verbose: bool):
    # pula se não houver coluna de função
    try:
        funcao_col = pick_first_existing(
            engine, schema, "fato_despesa",
            ["funcao", "nome_funcao", "funcao_nome", "descricao_funcao"]
        )
    except RuntimeError as e:
        log(f"ℹ️ Função indisponível no fato_despesa — pulando esta KPI ({e})", verbose)
        return

    if verbose:
        log(f"🔎 usando coluna de FUNÇÃO: {funcao_col}", True)

    sql = text(f"""
        WITH base AS (
          SELECT
            exercicio::int AS ano,
            {funcao_col} AS funcao,
            SUM(valor_empenhado) AS empenhado,
            SUM(valor_liquidado) AS liquidado,
            SUM(valor_pago)      AS pago
          FROM {schema}.fato_despesa
          WHERE exercicio = :ano
          GROUP BY 1,2
        )
        SELECT
          ano, funcao, empenhado, liquidado, pago,
          CASE WHEN SUM(pago) OVER () > 0
               THEN pago / SUM(pago) OVER ()
               ELSE 0::float END AS pago_share
        FROM base
        ORDER BY pago DESC;
    """)
    df = pd.read_sql(sql, engine, params={"ano": int(ano)})
    out_csv = outdir / str(ano) / "execucao_por_funcao_anual.csv"
    ensure_dir(out_csv.parent)
    df.to_csv(out_csv, index=False)
    log(f"📝 execucao_por_funcao_anual → {out_csv}", verbose)


def export_execucao_por_orgao_unidade_anual(engine: Engine, schema: str, outdir: Path, ano: int, verbose: bool):
    # pula se não houver orgao/unidade
    try:
        orgao_col = pick_first_existing(
            engine, schema, "fato_despesa",
            ["orgao", "nome_orgao", "orgao_nome", "descricao_orgao"]
        )
        unidade_col = pick_first_existing(
            engine, schema, "fato_despesa",
            ["unidade", "nome_unidade", "unidade_nome", "descricao_unidade"]
        )
    except RuntimeError as e:
        log(f"ℹ️ Órgão/Unidade indisponível no fato_despesa — pulando esta KPI ({e})", verbose)
        return

    if verbose:
        log(f"🔎 usando coluna de ÓRGÃO: {orgao_col} | UNIDADE: {unidade_col}", True)

    sql = text(f"""
        SELECT
          exercicio::int AS ano,
          {orgao_col}   AS orgao,
          {unidade_col} AS unidade,
          SUM(valor_empenhado) AS empenhado,
          SUM(valor_liquidado) AS liquidado,
          SUM(valor_pago)      AS pago
        FROM {schema}.fato_despesa
        WHERE exercicio = :ano
        GROUP BY 1,2,3
        ORDER BY pago DESC;
    """)
    df = pd.read_sql(sql, engine, params={"ano": int(ano)})
    out_csv = outdir / str(ano) / "execucao_por_orgao_unidade_anual.csv"
    ensure_dir(out_csv.parent)
    df.to_csv(out_csv, index=False)
    log(f"📝 execucao_por_orgao_unidade_anual → {out_csv}", verbose)


def export_receita_por_codigo_anual(engine: Engine, schema: str, outdir: Path, ano: int, verbose: bool):
    """Nova KPI aderente ao seu dado atual: receita agregada por 'codigo' do Anexo 10."""
    sql = text(f"""
        SELECT exercicio::int AS ano,
               codigo::text   AS codigo,
               SUM(previsao)    AS previsao,
               SUM(arrecadacao) AS arrecadacao
        FROM {schema}.fato_receita
        WHERE exercicio = :ano
        GROUP BY 1,2
        ORDER BY arrecadacao DESC;
    """)
    df = pd.read_sql(sql, engine, params={"ano": int(ano)})
    out_csv = outdir / str(ano) / "receita_por_codigo_anual.csv"
    out_json = outdir / str(ano) / "receita_por_codigo_anual.json"
    write_csv_and_json(df, out_csv, out_json)
    log(f"📝 receita_por_codigo_anual → {out_csv} | {out_json}", verbose)


# --------------------------
# Pipeline por ano
# --------------------------
def export_all_for_year(engine: Engine, schema: str, outdir: Path, ano: int, verbose: bool):
    # principais
    export_execucao_global_anual(engine, schema, outdir, ano, verbose)
    export_execucao_por_entidade_anual(engine, schema, outdir, ano, verbose)
    export_receita_prevista_arrecadada_anual(engine, schema, outdir, ano, verbose)
    export_superavit_deficit_anual(engine, schema, outdir, ano, verbose)

    # novos (somente quando existir dimensão correspondente)
    export_execucao_por_funcao_anual(engine, schema, outdir, ano, verbose)
    export_execucao_por_orgao_unidade_anual(engine, schema, outdir, ano, verbose)

    # nova KPI compatível com o dado atual
    export_receita_por_codigo_anual(engine, schema, outdir, ano, verbose)

    # validações e cobertura
    export_validations_fatos_vs_staging(engine, schema, outdir, ano, verbose)
    export_data_coverage(engine, schema, outdir, ano, verbose)


# --------------------------
# Main
# --------------------------
def main():
    args = parse_args()
    engine = get_engine()
    outdir = Path(args.outdir)
    ensure_dir(outdir)

    # Resolve anos
    if args.all_years:
        anos = discover_all_years(engine, args.schema)
    elif args.years:
        anos = parse_years_arg(args.years)
    elif args.year is not None:
        anos = [int(args.year)]
    else:
        print("ERROR: informe --all-years OU --years OU --year.", file=sys.stderr)
        sys.exit(2)

    if not anos:
        print("ERROR: nenhum ano para exportar.", file=sys.stderr)
        sys.exit(2)

    for ano in anos:
        export_all_for_year(engine, args.schema, outdir, int(ano), args.verbose)

    log("✅ KPIs exportados e validados (Fatos↔Staging; opcional RAW/QC).", True)


if __name__ == "__main__":
    main()
