#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
06_quality_checks.py — Quality checks do Monitor de Execução Orçamentária.

Regras implementadas:
  R1  Desigualdade despesa: pago ≤ liquidado ≤ empenhado (por exercicio, entidade)
  R2  Negativos em fatos (receita com whitelist p/ redutoras)
  R3  Duplicatas no staging (id_linha_hash)
  R4  Reconciliação: fatos x staging (anual) com conversão numérica robusta (pt-BR/US)
  R5  Cobertura de anos (fatos dentro do intervalo informado)
  R6  Anomalias YoY > threshold (default 30%) em cada métrica dos fatos
  R7  Linhas 'TOTAL' em stg_receitas (possível somatório indevido)

Saídas:
  outputs/quality/R{n}_*.csv + outputs/quality/SUMMARY.csv

Uso:
  export DATABASE_URL="postgresql://user:pass@host:5432/db"
  python scripts/06_quality_checks.py \
    --schema public --staging public \
    --years 2018-2025 \
    --reconcile-threshold 1.0 \
    --yoy-threshold 0.30 \
    --outdir outputs/quality \
    --verbose
"""

import argparse
import os
import sys
import unicodedata
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
from sqlalchemy import create_engine, text

# ================= Config =================

NEG_RECEITA_ALLOW = {
    "renúncia", "renuncia",
    "restituições", "restituicoes",
    "descontos concedidos",
    "outras deduções", "outras deducoes",
    "deduções de receita para a formação do fundeb",
    "deducoes de receita para a formacao do fundeb",
}

STAGING_TABLES = [
    "stg_despesas_empenhadas",
    "stg_despesas_liquidadas",
    "stg_despesas_pagas",
    "stg_receitas",
]

CRITICAL_FLAGS: List[str] = []
VERBOSE = False

# ================= Utils =================

def log(msg: str):
    if VERBOSE:
        print(msg)

def norm_txt(s: str) -> str:
    if s is None:
        return ""
    s = s.strip().lower()
    s = "".join(c for c in unicodedata.normalize("NFD", s) if unicodedata.category(c) != "Mn")
    return s

def norm_key(s: str) -> str:
    """normaliza para casar padrões de nomes de colunas (ex.: 'Líquido - Orçamento' -> 'liquido___orcamento')"""
    s = norm_txt(s)
    return "".join(ch if ch.isalnum() else "_" for ch in s)

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

def eng_from_env():
    url = os.getenv("DATABASE_URL")
    if not url:
        print("❌ Defina DATABASE_URL", file=sys.stderr)
        sys.exit(1)
    url = url.replace("postgresql+psycopg2://", "postgresql://")
    return create_engine(url, future=True)

def df_query(engine, sql: str, params: Optional[Dict[str, Any]] = None) -> pd.DataFrame:
    with engine.begin() as con:
        return pd.read_sql_query(text(sql), con, params=params or {})

def save_report(df: pd.DataFrame, path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False, encoding="utf-8")
    print(f"📝 Relatório salvo: {path}")

def to_numeric_sql(col_quoted: str) -> str:
    """
    Conversor SQL robusto pt-BR/US:
      - se houver vírgula → trata como pt-BR (remove pontos, vírgula -> ponto)
      - senão → mantém ponto como decimal
      - negativos com parênteses: (123,45) → -123.45
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

# --- introspecção dinâmica de colunas ---

def get_columns(engine, schema: str, table: str) -> List[str]:
    sql = """
      SELECT column_name
      FROM information_schema.columns
      WHERE table_schema = :s AND table_name = :t
      ORDER BY ordinal_position;
    """
    with engine.begin() as con:
        rows = con.execute(text(sql), {"s": schema, "t": table}).fetchall()
    return [r[0] for r in rows]

def find_col(cols: List[str], candidates: List[str]) -> Optional[str]:
    cmap = {norm_txt(c): c for c in cols}
    for cand in candidates:
        k = norm_txt(cand)
        if k in cmap:
            return cmap[k]
    # tenta prefixo
    for cand in candidates:
        k = norm_txt(cand)
        for nk, v in cmap.items():
            if nk.startswith(k):
                return v
    return None

def find_col_contains(cols: List[str], must_have: List[str]) -> Optional[str]:
    """
    Encontra a primeira coluna cujo nome normalizado contenha TODOS os termos em must_have.
    """
    nmap = {c: norm_key(c) for c in cols}
    for col, nk in nmap.items():
        ok = True
        for term in must_have:
            if term not in nk:
                ok = False
                break
        if ok:
            return col
    return None

def year_col(engine, schema: str, table: str) -> str:
    cols = get_columns(engine, schema, table)
    y = find_col(cols, ["exercicio", "ano"])
    if not y:
        raise RuntimeError(f"Não encontrei coluna de ano em {schema}.{table}. Colunas: {cols}")
    return y

def resolve_stg_amount_cols(engine, schema_stg: str) -> Dict[str, Optional[str]]:
    """
    Resolve nomes reais das colunas de valor em cada staging com base em padrões normalizados.
    Retorna dict com chaves:
      emp_liquido | emp_empenhado
      liq_orc, liq_rap
      pag_orc, pag_rap
    """
    out: Dict[str, Optional[str]] = {
        "emp_liquido": None, "emp_empenhado": None,
        "liq_orc": None, "liq_rap": None,
        "pag_orc": None, "pag_rap": None,
    }

    # EMPENHADAS
    cols_emp = get_columns(engine, schema_stg, "stg_despesas_empenhadas")
    out["emp_liquido"] = find_col_contains(cols_emp, ["liquido"])
    if not out["emp_liquido"]:
        # fallback: alguns portais chamam de 'empenhado'
        out["emp_empenhado"] = find_col_contains(cols_emp, ["empenhad"])

    # LIQUIDADAS
    cols_liq = get_columns(engine, schema_stg, "stg_despesas_liquidadas")
    out["liq_orc"] = (
        find_col_contains(cols_liq, ["liquid", "orcamento"]) or
        find_col_contains(cols_liq, ["liquido", "orcamento"])
    )
    out["liq_rap"] = (
        find_col_contains(cols_liq, ["liquid", "restos"]) or
        find_col_contains(cols_liq, ["liquid", "pagar"]) or
        find_col_contains(cols_liq, ["liquido", "restos"]) or
        find_col_contains(cols_liq, ["liquido", "pagar"])
    )

    # PAGAS
    cols_pag = get_columns(engine, schema_stg, "stg_despesas_pagas")
    out["pag_orc"] = (
        find_col_contains(cols_pag, ["pago", "orcamento"]) or
        find_col_contains(cols_pag, ["pago", "orc"])
    )
    out["pag_rap"] = (
        find_col_contains(cols_pag, ["pago", "restos"]) or
        find_col_contains(cols_pag, ["pago", "pagar"])
    )

    log(f"[R4] Colunas detectadas (empenhadas): {out['emp_liquido'] or out['emp_empenhado']}")
    log(f"[R4] Colunas detectadas (liquidadas): liq_orc={out['liq_orc']}, liq_rap={out['liq_rap']}")
    log(f"[R4] Colunas detectadas (pagas): pag_orc={out['pag_orc']}, pag_rap={out['pag_rap']}")
    return out

# ================= Regras =================

def r1_inequalities(engine, schema: str) -> pd.DataFrame:
    sql = f"""
      SELECT exercicio, entidade, valor_empenhado, valor_liquidado, valor_pago,
             CASE WHEN COALESCE(valor_pago,0) <= COALESCE(valor_liquidado,0)
                       AND COALESCE(valor_liquidado,0) <= COALESCE(valor_empenhado,0)
                  THEN 0 ELSE 1 END AS violacao
      FROM "{schema}"."fato_despesa";
    """
    df = df_query(engine, sql)
    return df[df["violacao"] == 1][["exercicio","entidade","valor_empenhado","valor_liquidado","valor_pago"]]

def r2_negatives(engine, schema: str) -> pd.DataFrame:
    qd = f"""
      SELECT 'fato_despesa' AS tabela, exercicio, entidade,
             'valor_empenhado' AS campo, valor_empenhado AS valor
      FROM "{schema}"."fato_despesa" WHERE valor_empenhado < 0
      UNION ALL
      SELECT 'fato_despesa', exercicio, entidade, 'valor_liquidado', valor_liquidado
      FROM "{schema}"."fato_despesa" WHERE valor_liquidado < 0
      UNION ALL
      SELECT 'fato_despesa', exercicio, entidade, 'valor_pago', valor_pago
      FROM "{schema}"."fato_despesa" WHERE valor_pago < 0;
    """
    df_d = df_query(engine, qd)

    qr = f"""
      SELECT 'fato_receita' AS tabela, exercicio, especificacao,
             'previsao' AS campo, previsao AS valor
      FROM "{schema}"."fato_receita" WHERE previsao < 0
      UNION ALL
      SELECT 'fato_receita', exercicio, especificacao, 'arrecadacao', arrecadacao
      FROM "{schema}"."fato_receita" WHERE arrecadacao < 0;
    """
    df_r = df_query(engine, qr)
    if not df_r.empty:
        df_r["_norm_espec"] = df_r["especificacao"].map(norm_txt)
        df_r = df_r[~df_r["_norm_espec"].isin(NEG_RECEITA_ALLOW)]
        df_r = df_r.drop(columns=["_norm_espec"])

    frames = [d for d in (df_d, df_r) if not d.empty]
    if not frames:
        return pd.DataFrame(columns=["tabela","exercicio","entidade","especificacao","campo","valor"])
    return pd.concat(frames, ignore_index=True)

def r3_dups_staging(engine, schema_stg: str) -> pd.DataFrame:
    frames = []
    for t in STAGING_TABLES:
        sql = f"""
          SELECT '{t}' AS tabela, id_linha_hash, COUNT(*) AS qtd
          FROM "{schema_stg}"."{t}"
          GROUP BY id_linha_hash
          HAVING COUNT(*) > 1;
        """
        try:
            df = df_query(engine, sql)
            if not df.empty:
                frames.append(df)
        except Exception:
            pass
    if not frames:
        return pd.DataFrame(columns=["tabela","id_linha_hash","qtd"])
    return pd.concat(frames, ignore_index=True)

def r4_reconcile_facts_vs_staging(engine, schema: str, schema_stg: str, years: Optional[List[int]], thr: float) -> pd.DataFrame:
    where_years = ""
    params: Dict[str, Any] = {}
    if years:
        where_years = "WHERE exercicio = ANY(:years)"
        params["years"] = years

    # --- FATO DESPESA (por ano)
    q_fd = f"""
      SELECT exercicio,
             SUM(COALESCE(valor_empenhado,0)) AS fato_empenhado,
             SUM(COALESCE(valor_liquidado,0)) AS fato_liquidado,
             SUM(COALESCE(valor_pago,0))      AS fato_pago
      FROM "{schema}"."fato_despesa"
      {where_years}
      GROUP BY exercicio
      ORDER BY exercicio;
    """
    fd = df_query(engine, q_fd, params)

    # Descobre a coluna de ano de cada staging e resolve colunas de valor dinamicamente
    y_emp = year_col(engine, schema_stg, "stg_despesas_empenhadas")
    y_liq = year_col(engine, schema_stg, "stg_despesas_liquidadas")
    y_pag = year_col(engine, schema_stg, "stg_despesas_pagas")
    y_rec = year_col(engine, schema_stg, "stg_receitas")

    amt = resolve_stg_amount_cols(engine, schema_stg)

    # --- STAGING DESPESAS (por ano)
    emp_val_expr = None
    if amt["emp_liquido"]:
        emp_val_expr = to_numeric_sql(f'"{amt["emp_liquido"]}"')
    elif amt["emp_empenhado"]:
        emp_val_expr = to_numeric_sql(f'"{amt["emp_empenhado"]}"')
    else:
        emp_val_expr = "0::numeric"  # não encontrado

    liq_orc_expr = to_numeric_sql(f'"{amt["liq_orc"]}"') if amt["liq_orc"] else "0::numeric"
    liq_rap_expr = to_numeric_sql(f'"{amt["liq_rap"]}"') if amt["liq_rap"] else "0::numeric"
    pag_orc_expr = to_numeric_sql(f'"{amt["pag_orc"]}"') if amt["pag_orc"] else "0::numeric"
    pag_rap_expr = to_numeric_sql(f'"{amt["pag_rap"]}"') if amt["pag_rap"] else "0::numeric"

    q_sd = f"""
      WITH emp AS (
        SELECT NULLIF("{y_emp}", '')::int AS ano,
               SUM(COALESCE({emp_val_expr},0)) AS empenhado
        FROM "{schema_stg}"."stg_despesas_empenhadas"
        GROUP BY 1
      ),
      liq AS (
        SELECT NULLIF("{y_liq}", '')::int AS ano,
               SUM(COALESCE({liq_orc_expr},0) + COALESCE({liq_rap_expr},0)) AS liquidado
        FROM "{schema_stg}"."stg_despesas_liquidadas"
        GROUP BY 1
      ),
      pag AS (
        SELECT NULLIF("{y_pag}", '')::int AS ano,
               SUM(COALESCE({pag_orc_expr},0) + COALESCE({pag_rap_expr},0)) AS pago
        FROM "{schema_stg}"."stg_despesas_pagas"
        GROUP BY 1
      )
      SELECT COALESCE(e.ano,l.ano,p.ano) AS exercicio,
             e.empenhado, l.liquidado, p.pago
      FROM emp e
      FULL JOIN liq l ON l.ano = e.ano
      FULL JOIN pag p ON p.ano = COALESCE(e.ano,l.ano)
      ORDER BY 1;
    """
    sd = df_query(engine, q_sd)

    # --- FATO RECEITA (por ano)
    q_fr = f"""
      SELECT exercicio,
             SUM(COALESCE(previsao,0))    AS fato_previsao,
             SUM(COALESCE(arrecadacao,0)) AS fato_arrecadacao
      FROM "{schema}"."fato_receita"
      {where_years}
      GROUP BY exercicio
      ORDER BY exercicio;
    """
    fr = df_query(engine, q_fr, params)

    # --- STAGING RECEITA (por ano) — usa coluna dinâmica e conversão robusta
    q_sr = f"""
      SELECT NULLIF("{y_rec}", '')::int AS exercicio,
             SUM(COALESCE({to_numeric_sql('"previsao"')},0))    AS stg_previsao,
             SUM(COALESCE({to_numeric_sql('"arrecadacao"')},0)) AS stg_arrecadacao
      FROM "{schema_stg}"."stg_receitas"
      GROUP BY 1
      ORDER BY 1;
    """
    sr = df_query(engine, q_sr)

    # join e diferenças
    out = []
    anos = sorted(set(fd.get("exercicio", [])).union(sd.get("exercicio", [])).union(fr.get("exercicio", [])).union(sr.get("exercicio", [])))
    if years:
        anos = [y for y in anos if y in years]

    def gv(df: pd.DataFrame, y: int, col: str) -> float:
        try:
            return float(df.loc[df["exercicio"] == y, col].iloc[0])
        except Exception:
            return float("nan")

    for y in anos:
        linha = {
            "exercicio": y,
            "fato_empenhado": gv(fd, y, "fato_empenhado"),
            "stg_empenhado": gv(sd, y, "empenhado"),
            "fato_liquidado": gv(fd, y, "fato_liquidado"),
            "stg_liquidado": gv(sd, y, "liquidado"),
            "fato_pago": gv(fd, y, "fato_pago"),
            "stg_pago": gv(sd, y, "pago"),
            "fato_previsao": gv(fr, y, "fato_previsao"),
            "stg_previsao": gv(sr, y, "stg_previsao"),
            "fato_arrecadacao": gv(fr, y, "fato_arrecadacao"),
            "stg_arrecadacao": gv(sr, y, "stg_arrecadacao"),
        }
        for a, b, name in [
            ("fato_empenhado","stg_empenhado","diff_emp"),
            ("fato_liquidado","stg_liquidado","diff_liq"),
            ("fato_pago","stg_pago","diff_pag"),
            ("fato_previsao","stg_previsao","diff_prev"),
            ("fato_arrecadacao","stg_arrecadacao","diff_arr"),
        ]:
            va, vb = linha[a], linha[b]
            linha[name] = abs(va - vb) if pd.notna(va) and pd.notna(vb) else None
        out.append(linha)

    df = pd.DataFrame(out)
    if not df.empty:
        cols_diff = ["diff_emp","diff_liq","diff_pag","diff_prev","diff_arr"]
        mask = False
        for c in cols_diff:
            mask = (mask | (df[c].fillna(0) >= thr)) if isinstance(mask, pd.Series) else (df[c].fillna(0) >= thr)
        df = df[mask] if isinstance(mask, pd.Series) else df
    return df

def r5_year_coverage(engine, schema: str, years: Optional[List[int]]) -> pd.DataFrame:
    if not years:
        return pd.DataFrame(columns=["tabela","ano_ausente"])
    q1 = f'SELECT DISTINCT exercicio FROM "{schema}"."fato_despesa";'
    q2 = f'SELECT DISTINCT exercicio FROM "{schema}"."fato_receita";'
    fd = set(df_query(engine, q1)["exercicio"].tolist())
    fr = set(df_query(engine, q2)["exercicio"].tolist())
    rows = []
    for y in years:
        if y not in fd: rows.append({"tabela":"fato_despesa","ano_ausente":y})
        if y not in fr: rows.append({"tabela":"fato_receita","ano_ausente":y})
    return pd.DataFrame(rows, columns=["tabela","ano_ausente"])

def r6_yoy_anomalies(engine, schema: str, yoy_thr: float) -> pd.DataFrame:
    qd = f"""
      WITH agg AS (
        SELECT exercicio,
               SUM(valor_empenhado) AS v_emp,
               SUM(valor_liquidado) AS v_liq,
               SUM(valor_pago)      AS v_pag
        FROM "{schema}"."fato_despesa"
        GROUP BY exercicio
      ),
      r AS (
        SELECT a.exercicio,
               a.v_emp, a.v_liq, a.v_pag,
               LAG(a.v_emp) OVER (ORDER BY a.exercicio) AS v_emp_prev,
               LAG(a.v_liq) OVER (ORDER BY a.exercicio) AS v_liq_prev,
               LAG(a.v_pag) OVER (ORDER BY a.exercicio) AS v_pag_prev
        FROM agg a
      )
      SELECT exercicio,
             v_emp, v_emp_prev,
             v_liq, v_liq_prev,
             v_pag, v_pag_prev
      FROM r
      ORDER BY exercicio;
    """
    d = df_query(engine, qd)

    qr = f"""
      WITH agg AS (
        SELECT exercicio,
               SUM(previsao)    AS v_prev,
               SUM(arrecadacao) AS v_arr
        FROM "{schema}"."fato_receita"
        GROUP BY exercicio
      ),
      r AS (
        SELECT a.exercicio,
               a.v_prev, a.v_arr,
               LAG(a.v_prev) OVER (ORDER BY a.exercicio) AS v_prev_prev,
               LAG(a.v_arr) OVER (ORDER BY a.exercicio) AS v_arr_prev
        FROM agg a
      )
      SELECT exercicio,
             v_prev, v_prev_prev,
             v_arr,  v_arr_prev
      FROM r
      ORDER BY exercicio;
    """
    r = df_query(engine, qr)

    rows = []

    def check(df, cur, prev, label):
        for _, row in df.iterrows():
            v = row[cur]; p = row[prev]
            if pd.notna(v) and pd.notna(p) and p != 0:
                yoy = abs((v - p) / p)
                if yoy >= yoy_thr:
                    rows.append({"tabela": label, "exercicio": int(row["exercicio"]), "yoy_abs": float(yoy), "valor": float(v), "valor_ano_anterior": float(p)})

    if not d.empty:
        check(d, "v_emp", "v_emp_prev", "fato_despesa_empenhado")
        check(d, "v_liq", "v_liq_prev", "fato_despesa_liquidado")
        check(d, "v_pag", "v_pag_prev", "fato_despesa_pago")
    if not r.empty:
        check(r, "v_prev", "v_prev_prev", "fato_receita_previsao")
        check(r, "v_arr",  "v_arr_prev",  "fato_receita_arrecadacao")

    return pd.DataFrame(rows, columns=["tabela","exercicio","yoy_abs","valor","valor_ano_anterior"])

def r7_total_rows_in_receita(engine, schema_stg: str) -> pd.DataFrame:
    sql = f"""
      SELECT *
      FROM "{schema_stg}"."stg_receitas"
      WHERE UPPER(COALESCE(codigo, '')) = 'TOTAL'
         OR UPPER(COALESCE(especificacao, '')) = 'TOTAL';
    """
    try:
        df = df_query(engine, sql)
    except Exception:
        df = pd.DataFrame()
    return df

# ================= Main =================

def main():
    global VERBOSE

    ap = argparse.ArgumentParser()
    ap.add_argument("--schema", default="public")
    ap.add_argument("--staging", default="public")
    ap.add_argument("--years", "--year", dest="years", help="Faixa ou lista: 2018-2025 ou 2018,2019")
    ap.add_argument("--outdir", default="outputs/quality")
    ap.add_argument("--reconcile-threshold", type=float, default=1.0, help="mínimo abs. p/ sinalizar difs em R4")
    ap.add_argument("--yoy-threshold", type=float, default=0.30, help="limiar YoY (ex.: 0.30 = 30%) em R6")
    ap.add_argument("--verbose", action="store_true")
    args = ap.parse_args()

    VERBOSE = args.verbose

    engine = eng_from_env()
    outdir = Path(args.outdir)
    years = parse_years(args.years)

    reports = []

    # R1
    r1 = r1_inequalities(engine, args.schema)
    if not r1.empty:
        CRITICAL_FLAGS.append("R1")
        save_report(r1, outdir / "R1_inequalities.csv")
        reports.append(("R1", len(r1)))

    # R2
    r2 = r2_negatives(engine, args.schema)
    if not r2.empty:
        CRITICAL_FLAGS.append("R2")
        save_report(r2, outdir / "R2_negativos.csv")
        reports.append(("R2", len(r2)))

    # R3
    r3 = r3_dups_staging(engine, args.staging)
    if not r3.empty:
        save_report(r3, outdir / "R3_duplicatas_staging.csv")
        reports.append(("R3", len(r3)))

    # R4
    r4 = r4_reconcile_facts_vs_staging(engine, args.schema, args.staging, years, args.reconcile_threshold)
    if not r4.empty:
        CRITICAL_FLAGS.append("R4")
        save_report(r4, outdir / "R4_reconcile_fatos_vs_staging.csv")
        reports.append(("R4", len(r4)))

    # R5
    r5 = r5_year_coverage(engine, args.schema, years)
    if not r5.empty:
        save_report(r5, outdir / "R5_cobertura_anos.csv")
        reports.append(("R5", len(r5)))

    # R6
    r6 = r6_yoy_anomalies(engine, args.schema, args.yoy_threshold)
    if not r6.empty:
        save_report(r6, outdir / "R6_yoy_anomalias.csv")
        reports.append(("R6", len(r6)))

    # R7
    r7 = r7_total_rows_in_receita(engine, args.staging)
    if not r7.empty:
        save_report(r7, outdir / "R7_receita_linhas_TOTAL.csv")
        reports.append(("R7", len(r7)))

    # Summary
    if reports:
        summary = pd.DataFrame(reports, columns=["regra","qtd_registros"])
        save_report(summary, outdir / "SUMMARY.csv")
    else:
        print("✅ Nenhuma inconsistência encontrada nos checks executados.")

    # Exit code — crítico se R1 ou R4
    if any(r in CRITICAL_FLAGS for r in ("R1","R4")):
        sys.exit(1)

if __name__ == "__main__":
    main()
