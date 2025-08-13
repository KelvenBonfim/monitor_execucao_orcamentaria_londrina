#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
09_export_kpis.py — KPIs anuais a partir dos Fatos (DB-first) com validação contra STAGING e, opcionalmente, RAW/quality.

Fontes obrigatórias (no mesmo --schema):
  • fato_despesa(exercicio, [entidade, orgao, unidade, funcao], valor_empenhado, valor_liquidado, valor_pago)
  • fato_receita(exercicio, previsao|valor_previsto, arrecadacao|valor_arrecadado)
  • stg_despesas_empenhadas / stg_despesas_liquidadas / stg_despesas_pagas / stg_receitas (para validação)

Fontes opcionais:
  • --rawdir: diretório com CSVs brutos (raw/empenhadas|liquidadas|pagas e raw/receitas/anexo10_prev_arrec_<ANO>.csv)
  • --qcdir : diretório com CSVs de reconcile/quality (ex.: outputs/reconcile_raw_vs_portal, outputs/quality)

Saídas por ano (CSV + JSON) em --outdir/<ano>/ :
  - execucao_global_anual
  - execucao_por_entidade_anual                 (se houver coluna)
  - execucao_por_orgao_unidade_anual            (se houver colunas)
  - execucao_por_funcao_anual                   (se houver coluna)
  - receita_prevista_arrecadada_anual
  - superavit_deficit_anual
  - ranking_funcoes_top_crescimento / _top_queda
  - ranking_orgaos_top_crescimento  / _top_queda
  - validations_fatos_vs_staging                (diferenças anuais)
  - validations_staging_vs_raw                  (se --rawdir)
  - data_coverage_report.json                   (resumo + flags de qualidade/reconcile)

Uso (DB):
  export DATABASE_URL="postgresql://user:pass@host:5432/db"
  python scripts/09_export_kpis.py --schema public --staging public --all-years --outdir outputs/kpis

Uso (com RAW/QC):
  python scripts/09_export_kpis.py \
    --schema public --staging public --all-years \
    --rawdir raw --qcdir outputs \
    --outdir outputs/kpis

Observações:
  - Este script usa descoberta dinâmica de colunas e conversão numérica robusta (pt-BR/US), como nos scripts 05/06/07.
"""

from __future__ import annotations
import argparse
import json
import os
import re
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd
from sqlalchemy import create_engine, text

# ============================ Utils base ============================

def eng_from_env():
    url = os.getenv("DATABASE_URL")
    if not url:
        raise SystemExit("❌ Defina DATABASE_URL")
    return create_engine(url.replace("postgresql+psycopg2://", "postgresql://"), future=True)

def write(df: pd.DataFrame, base: Path, name: str):
    base.mkdir(parents=True, exist_ok=True)
    df.to_csv(base / f"{name}.csv", index=False, encoding="utf-8")
    df.to_json(base / f"{name}.json", orient="records", force_ascii=False)
    print(f"📝 {name} → {base}/{name}.csv | {name}.json")

def df_sql(engine, sql: str, params=None) -> pd.DataFrame:
    with engine.begin() as con:
        return pd.read_sql_query(text(sql), con, params=params or {})

# normalização de nomes/pt-BR semelhante aos scripts 05/06/07
import unicodedata

def norm_txt(s: str) -> str:
    if s is None: return ""
    s = s.strip().lower()
    s = "".join(c for c in unicodedata.normalize("NFD", s) if unicodedata.category(c) != "Mn")
    return s

def norm_key(s: str) -> str:
    s = norm_txt(s)
    return re.sub(r"[^a-z0-9]+", "_", s)

# mesma ideia de conversor numérico dos scripts 05/06/07 (em SQL)
def to_numeric_sql(col_q: str) -> str:
    return f"""
    (
      CASE
        WHEN {col_q} IS NULL OR {col_q} = '' THEN 0::numeric
        ELSE (CASE WHEN {col_q} LIKE '(%%' AND {col_q} LIKE '%%)' THEN -1 ELSE 1 END) * (
          CASE WHEN {col_q} LIKE '%%,%%' THEN
            NULLIF(REPLACE(REPLACE(REGEXP_REPLACE({col_q}, '[^0-9,().-]', '', 'g'),'.',''),',','.'),'')::numeric
          ELSE
            NULLIF(REGEXP_REPLACE({col_q}, '[^0-9().-]', '', 'g'),'')::numeric
          END)
      END
    )
    """

# ============================ DB helpers ============================

def columns(engine, schema: str, table: str) -> List[str]:
    sql = """
      SELECT column_name
      FROM information_schema.columns
      WHERE table_schema=:s AND table_name=:t
      ORDER BY ordinal_position;
    """
    with engine.begin() as con:
        rows = con.execute(text(sql), {"s": schema, "t": table}).fetchall()
    return [r[0] for r in rows]

def find_col(cols: List[str], candidates: List[str]) -> Optional[str]:
    cmap = {norm_txt(c): c for c in cols}
    for cand in candidates:
        k = norm_txt(cand)
        if k in cmap: return cmap[k]
    for cand in candidates:
        k = norm_txt(cand)
        for nk, v in cmap.items():
            if nk.startswith(k):
                return v
    return None

def find_col_contains(cols: List[str], must_have: List[str]) -> Optional[str]:
    nmap = {c: norm_key(c) for c in cols}
    for col, nk in nmap.items():
        if all(t in nk for t in must_have):
            return col
    return None

# ============================ Leitura Fatos ============================

def read_fatos(engine, schema: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
    fd = df_sql(engine, f'SELECT * FROM "{schema}"."fato_despesa";')
    fr = df_sql(engine, f'SELECT * FROM "{schema}"."fato_receita";')
    return fd, fr

# ============================ KPIs (anuais) ============================

def kpi_execucao_global_anual(fd: pd.DataFrame) -> pd.DataFrame:
    if fd.empty: return fd
    e = [c for c in fd.columns if c.lower()=="exercicio"][0]
    emp = [c for c in fd.columns if c.lower()=="valor_empenhado"][0]
    liq = [c for c in fd.columns if c.lower()=="valor_liquidado"][0]
    pag = [c for c in fd.columns if c.lower()=="valor_pago"][0]
    out = fd.groupby(fd[e].astype(int))[[emp, liq, pag]].sum().reset_index()
    out.columns = ["ano","empenhado","liquidado","pago"]
    out["pct_pago_sobre_empenhado"] = (out["pago"] / out["empenhado"]).replace([float("inf")], 0)
    out["pct_liquidado_sobre_empenhado"] = (out["liquidado"] / out["empenhado"]).replace([float("inf")], 0)
    return out

def kpi_execucao_por_entidade_anual(fd: pd.DataFrame) -> pd.DataFrame:
    cols = {c.lower() for c in fd.columns}
    need = {"exercicio","entidade","valor_pago"}
    if not need.issubset(cols): return pd.DataFrame()
    e = [c for c in fd.columns if c.lower()=="exercicio"][0]
    ent = [c for c in fd.columns if c.lower()=="entidade"][0]
    emp = [c for c in fd.columns if c.lower()=="valor_empenhado"][0]
    liq = [c for c in fd.columns if c.lower()=="valor_liquidado"][0]
    pag = [c for c in fd.columns if c.lower()=="valor_pago"][0]
    out = fd.groupby([fd[e].astype(int), fd[ent]])[[emp, liq, pag]].sum().reset_index()
    out.columns = ["ano","entidade","empenhado","liquidado","pago"]
    tot = out.groupby("ano")["pago"].transform("sum")
    out["pago_share"] = out["pago"] / tot
    return out

def kpi_execucao_por_orgao_unidade_anual(fd: pd.DataFrame) -> pd.DataFrame:
    cols = {c.lower() for c in fd.columns}
    need = {"exercicio","orgao","unidade","valor_pago"}
    if not need.issubset(cols): return pd.DataFrame()
    e = [c for c in fd.columns if c.lower()=="exercicio"][0]
    org = [c for c in fd.columns if c.lower()=="orgao"][0]
    uni = [c for c in fd.columns if c.lower()=="unidade"][0]
    emp = [c for c in fd.columns if c.lower()=="valor_empenhado"][0]
    liq = [c for c in fd.columns if c.lower()=="valor_liquidado"][0]
    pag = [c for c in fd.columns if c.lower()=="valor_pago"][0]
    out = fd.groupby([fd[e].astype(int), fd[org], fd[uni]])[[emp, liq, pag]].sum().reset_index()
    out.columns = ["ano","orgao","unidade","empenhado","liquidado","pago"]
    tot = out.groupby("ano")["pago"].transform("sum")
    out["pago_share"] = out["pago"] / tot
    return out

def kpi_execucao_por_funcao_anual(fd: pd.DataFrame) -> pd.DataFrame:
    cols = {c.lower() for c in fd.columns}
    need = {"exercicio","funcao","valor_pago"}
    if not need.issubset(cols): return pd.DataFrame()
    e = [c for c in fd.columns if c.lower()=="exercicio"][0]
    fn = [c for c in fd.columns if c.lower()=="funcao"][0]
    emp = [c for c in fd.columns if c.lower()=="valor_empenhado"][0]
    liq = [c for c in fd.columns if c.lower()=="valor_liquidado"][0]
    pag = [c for c in fd.columns if c.lower()=="valor_pago"][0]
    out = fd.groupby([fd[e].astype(int), fd[fn]])[[emp, liq, pag]].sum().reset_index()
    out.columns = ["ano","funcao","empenhado","liquidado","pago"]
    tot = out.groupby("ano")["pago"].transform("sum")
    out["pago_share"] = out["pago"] / tot
    return out

# Receita (descobre nomes das colunas)

def pick_receita_cols(df: pd.DataFrame) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    if df.empty: return None, None, None
    cols = list(df.columns)
    e = [c for c in cols if c.lower()=="exercicio"]
    prev = [c for c in cols if c.lower() in ("previsao","valor_previsto")]
    arr  = [c for c in cols if c.lower() in ("arrecadacao","valor_arrecadado")]
    return (e[0] if e else None, prev[0] if prev else None, arr[0] if arr else None)

def kpi_receita_prevista_arrecadada_anual(fr: pd.DataFrame) -> pd.DataFrame:
    if fr.empty: return fr
    e, prev, arr = pick_receita_cols(fr)
    if not e or not prev or not arr: return pd.DataFrame()
    out = fr.groupby(fr[e].astype(int))[[prev, arr]].sum().reset_index()
    out.columns = ["ano","previsto","arrecadado"]
    out["gap"] = out["previsto"] - out["arrecadado"]
    out["gap_pct"] = (out["gap"] / out["previsto"]).replace([float("inf")], 0)
    return out

# Rankings simples por variação do último ano disponível

def build_rankings(df: pd.DataFrame, grupo_col: str, valor_col: str, top_n: int):
    if df.empty or not {grupo_col, "ano", valor_col}.issubset(df.columns):
        return pd.DataFrame(), pd.DataFrame()
    base = df[["ano", grupo_col, valor_col]].sort_values(["ano", grupo_col]).copy()
    base["yoy_abs"] = base.groupby(grupo_col)[valor_col].diff()
    last = base.sort_values("ano").groupby(grupo_col).tail(1)
    top_up   = last.sort_values("yoy_abs", ascending=False).head(top_n)
    top_down = last.sort_values("yoy_abs", ascending=True).head(top_n)
    return top_up, top_down

# ===================== Validação: Fatos x Staging =====================

def stg_despesa_totais(engine, schema_stg: str, years: List[int]) -> pd.DataFrame:
    cols_emp = columns(engine, schema_stg, "stg_despesas_empenhadas")
    cols_liq = columns(engine, schema_stg, "stg_despesas_liquidadas")
    cols_pag = columns(engine, schema_stg, "stg_despesas_pagas")

    y_emp = find_col(cols_emp, ["exercicio","ano"]) or "exercicio"
    y_liq = find_col(cols_liq, ["exercicio","ano"]) or "exercicio"
    y_pag = find_col(cols_pag, ["exercicio","ano"]) or "exercicio"

    emp_val = find_col_contains(cols_emp, ["liquido"]) or find_col_contains(cols_emp, ["empenhad"])  # líquido ou empenhado
    liq_orc = find_col_contains(cols_liq, ["liquid","orcamento"]) or find_col_contains(cols_liq, ["liquido","orcamento"]) 
    liq_rap = find_col_contains(cols_liq, ["liquid","restos"]) or find_col_contains(cols_liq, ["liquid","pagar"]) or find_col_contains(cols_liq, ["liquido","restos"]) or find_col_contains(cols_liq, ["liquido","pagar"]) 
    pag_orc = find_col_contains(cols_pag, ["pago","orcamento"]) or find_col_contains(cols_pag, ["pago","orc"]) 
    pag_rap = find_col_contains(cols_pag, ["pago","restos"]) or find_col_contains(cols_pag, ["pago","pagar"]) 

    years_sql = "(" + ",".join(str(y) for y in years) + ")"

    sql = f"""
      WITH emp AS (
        SELECT NULLIF("{y_emp}",'')::int AS ano,
               SUM(COALESCE({to_numeric_sql(f'"{emp_val}"') if emp_val else '0::numeric'},0)) AS empenhado
        FROM "{schema_stg}"."stg_despesas_empenhadas"
        WHERE NULLIF("{y_emp}",'')::int IN {years_sql}
        GROUP BY 1
      ), liq AS (
        SELECT NULLIF("{y_liq}",'')::int AS ano,
               SUM(COALESCE({to_numeric_sql(f'"{liq_orc}"') if liq_orc else '0::numeric'},0) +
                   COALESCE({to_numeric_sql(f'"{liq_rap}"') if liq_rap else '0::numeric'},0)) AS liquidado
        FROM "{schema_stg}"."stg_despesas_liquidadas"
        WHERE NULLIF("{y_liq}",'')::int IN {years_sql}
        GROUP BY 1
      ), pag AS (
        SELECT NULLIF("{y_pag}",'')::int AS ano,
               SUM(COALESCE({to_numeric_sql(f'"{pag_orc}"') if pag_orc else '0::numeric'},0) +
                   COALESCE({to_numeric_sql(f'"{pag_rap}"') if pag_rap else '0::numeric'},0)) AS pago
        FROM "{schema_stg}"."stg_despesas_pagas"
        WHERE NULLIF("{y_pag}",'')::int IN {years_sql}
        GROUP BY 1
      )
      SELECT COALESCE(emp.ano, liq.ano, pag.ano) AS ano,
             emp.empenhado, liq.liquidado, pag.pago
      FROM emp
      FULL JOIN liq ON liq.ano = emp.ano
      FULL JOIN pag ON pag.ano = COALESCE(emp.ano, liq.ano)
      ORDER BY 1;"""
    return df_sql(engine, sql)

def stg_receita_totais(engine, schema_stg: str, years: List[int]) -> pd.DataFrame:
    cols = columns(engine, schema_stg, "stg_receitas")
    y = find_col(cols, ["exercicio","ano"]) or "exercicio"
    prev = find_col(cols, ["previsao","previsão"]) or "previsao"
    arr  = find_col(cols, ["arrecadacao","arrecadação"]) or "arrecadacao"
    years_sql = "(" + ",".join(str(x) for x in years) + ")"
    sql = f"""
      SELECT NULLIF("{y}",'')::int AS ano,
             SUM(COALESCE({to_numeric_sql(f'"{prev}"')},0)) AS previsto,
             SUM(COALESCE({to_numeric_sql(f'"{arr}"')},0))  AS arrecadado
      FROM "{schema_stg}"."stg_receitas"
      WHERE NULLIF("{y}",'')::int IN {years_sql}
      GROUP BY 1
      ORDER BY 1;"""
    return df_sql(engine, sql)

# ===================== RAW (opcional) =====================

MONEY_BR_RE = re.compile(r"-?\(?\s*(?:\d{1,3}(?:\.\d{3})*|\d+),(?:\d{2})\s*\)?")

def to_float_ptbr(x: str) -> Optional[float]:
    if x is None: return None
    s = str(x).strip()
    if s == "" or s == "-": return None
    neg = s.startswith("(") and s.endswith(")")
    s = s.replace(".", "").replace("\xa0", "").replace(" ", "").replace(",", ".")
    try:
        v = float(re.sub(r"[^0-9.\-]", "", s))
        return -v if neg else v
    except Exception:
        return None

def raw_totais_for_year(rawdir: Path, ano: int) -> Dict[str, float]:
    out = {"empenhado": 0.0, "liquidado": 0.0, "pago": 0.0, "previsto": 0.0, "arrecadado": 0.0}
    # despesas
    for stage, key in [("empenhadas","empenhado"),("liquidadas","liquidado"),("pagas","pago")]:
        folder = rawdir / stage
        if folder.exists():
            for p in folder.glob(f"**/*{ano}*.csv"):
                try:
                    df = pd.read_csv(p)
                except Exception:
                    df = pd.read_csv(p, sep=";")
                # autodetecta coluna de valor (pega a maior soma compatível)
                best_sum = 0.0
                for c in df.columns:
                    s = pd.to_numeric(df[c].apply(to_float_ptbr), errors="coerce").sum()
                    if abs(s) > abs(best_sum):
                        best_sum = float(s)
                out[key] += best_sum
    # receitas
    rdir = rawdir / "receitas"
    for p in [rdir / f"anexo10_prev_arrec_{ano}.csv"]:
        if p.exists():
            try:
                df = pd.read_csv(p)
            except Exception:
                df = pd.read_csv(p, sep=";")
            cand_prev = None; cand_arr = None
            for c in df.columns:
                ck = c.lower()
                if "previs" in ck: cand_prev = c
                if "arrecad" in ck: cand_arr = c
            if cand_prev and cand_arr:
                out["previsto"] += pd.to_numeric(df[cand_prev].apply(to_float_ptbr), errors="coerce").sum()
                out["arrecadado"] += pd.to_numeric(df[cand_arr].apply(to_float_ptbr), errors="coerce").sum()
    return out

# =============================== MAIN ===============================

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--schema", default="public")
    ap.add_argument("--staging", default="public")
    ap.add_argument("--year", type=int)
    ap.add_argument("--all-years", action="store_true")
    ap.add_argument("--outdir", default="outputs/kpis")
    ap.add_argument("--top-n", type=int, default=20)
    ap.add_argument("--despesa-metrica", choices=["pago","liquidado","empenhado"], default="pago")
    ap.add_argument("--rawdir", help="Pasta com CSVs RAW para conferência (ex.: raw)")
    ap.add_argument("--qcdir", help="Pasta raiz com outputs de quality/reconcile (opcional)")
    args = ap.parse_args()

    eng = eng_from_env()

    # Leitura Fatos
    fd, fr = read_fatos(eng, args.schema)
    if fd.empty:
        raise SystemExit("❌ fato_despesa vazio ou inexistente.")
    years_fd = sorted(pd.Series(fd[[c for c in fd.columns if c.lower()=="exercicio"][0]]).dropna().astype(int).unique().tolist())

    # Determinar anos alvo
    if args.year and not args.all_years:
        years = [args.year]
    else:
        years = years_fd

    if not years:
        print("⚠️ Nenhum ano encontrado em fato_despesa.")
        return

    outdir = Path(args.outdir)

    # Pré-calcular séries completas para YoY/rankings
    serie_glob = kpi_execucao_global_anual(fd)
    serie_rec  = kpi_receita_prevista_arrecadada_anual(fr)

    # Staging agregada (para validação)
    stg_d = stg_despesa_totais(eng, args.staging, years)
    stg_r = stg_receita_totais(eng, args.staging, years)

    for yr in years:
        sub = outdir / f"{yr}"
        # --- filtros por ano (fatos) ---
        e = [c for c in fd.columns if c.lower()=="exercicio"][0]
        fd_y = fd[fd[e].astype(int) == int(yr)]
        fr_y = fr
        if not fr.empty and any(c.lower()=="exercicio" for c in fr.columns):
            e2 = [c for c in fr.columns if c.lower()=="exercicio"][0]
            fr_y = fr[fr[e2].astype(int) == int(yr)]

        # --- KPIs ---
        glob_y = kpi_execucao_global_anual(fd_y)
        if not glob_y.empty:
            # merge com série completa (para YoY/linhas do ano mantendo contexto)
            aux = serie_glob.copy()
            glob_y = glob_y.merge(aux, on=["ano","empenhado","liquidado","pago","pct_pago_sobre_empenhado","pct_liquidado_sobre_empenhado"], how="left")
            write(glob_y, sub, "execucao_global_anual")

        ent_y = kpi_execucao_por_entidade_anual(fd_y)
        if not ent_y.empty:
            write(ent_y.sort_values(["ano","pago"], ascending=[True,False]), sub, "execucao_por_entidade_anual")

        ou_y = kpi_execucao_por_orgao_unidade_anual(fd_y)
        if not ou_y.empty:
            write(ou_y.sort_values(["ano","pago"], ascending=[True,False]), sub, "execucao_por_orgao_unidade_anual")

        fun_y = kpi_execucao_por_funcao_anual(fd_y)
        if not fun_y.empty:
            write(fun_y.sort_values(["ano","pago"], ascending=[True,False]), sub, "execucao_por_funcao_anual")
            # rankings por função usando série completa
            all_fun = kpi_execucao_por_funcao_anual(fd)
            up, down = build_rankings(all_fun, "funcao", "pago", top_n=args.top_n)
            if not up.empty: write(up, sub, "ranking_funcoes_top_crescimento")
            if not down.empty: write(down, sub, "ranking_funcoes_top_queda")

        rec_y = kpi_receita_prevista_arrecadada_anual(fr_y)
        if not rec_y.empty:
            write(rec_y, sub, "receita_prevista_arrecadada_anual")

        # Superávit/Déficit simples (receita arrecadada - despesa <métrica>)
        if not glob_y.empty and not rec_y.empty:
            m = args.despesa_metrica
            base_sd = glob_y[["ano", m]].merge(rec_y[["ano","arrecadado"]], on="ano", how="inner")
            base_sd["superavit_deficit"] = base_sd["arrecadado"] - base_sd[m]
            base_sd["resultado_pct_despesa"] = (base_sd["superavit_deficit"] / base_sd[m]).replace([float("inf")], 0)
            base_sd["resultado_pct_receita"] = (base_sd["superavit_deficit"] / base_sd["arrecadado"]).replace([float("inf")], 0)
            write(base_sd, sub, "superavit_deficit_anual")

        # Rankings por órgão (se houver orgao)
        if not ou_y.empty:
            all_ou = kpi_execucao_por_orgao_unidade_anual(fd)
            if not all_ou.empty:
                g = all_ou.groupby(["ano","orgao"], as_index=False).agg({"pago":"sum"})
                up, down = build_rankings(g, "orgao", "pago", top_n=args.top_n)
                if not up.empty: write(up, sub, "ranking_orgaos_top_crescimento")
                if not down.empty: write(down, sub, "ranking_orgaos_top_queda")

        # --- Validações: Fatos vs Staging ---
        val_fd = glob_y.copy()
        val_stg_d = stg_d[stg_d["ano"] == yr] if not stg_d.empty else pd.DataFrame()
        val_stg_r = stg_r[stg_r["ano"] == yr] if not stg_r.empty else pd.DataFrame()
        out_val = pd.DataFrame({"ano":[yr]})
        if not val_fd.empty:
            row_fd = val_fd.iloc[0]
            out_val["fd_empenhado"] = row_fd.get("empenhado")
            out_val["fd_liquidado"] = row_fd.get("liquidado")
            out_val["fd_pago"]      = row_fd.get("pago")
        if not val_stg_d.empty:
            r = val_stg_d.iloc[0]
            out_val["stg_empenhado"] = r.get("empenhado")
            out_val["stg_liquidado"] = r.get("liquidado")
            out_val["stg_pago"]      = r.get("pago")
        if not rec_y.empty:
            rr = rec_y.iloc[0]
            out_val["fd_previsto"]   = rr.get("previsto")
            out_val["fd_arrecadado"] = rr.get("arrecadado")
        if not val_stg_r.empty:
            r2 = val_stg_r.iloc[0]
            out_val["stg_previsto"]   = r2.get("previsto")
            out_val["stg_arrecadado"] = r2.get("arrecadado")
        # diffs
        for a,b in [("fd_empenhado","stg_empenhado"),("fd_liquidado","stg_liquidado"),("fd_pago","stg_pago"),
                    ("fd_previsto","stg_previsto"),("fd_arrecadado","stg_arrecadado")]:
            if a in out_val and b in out_val:
                out_val[f"diff_{a.split('_',1)[1]}"] = float(out_val[a] - out_val[b])
        write(out_val, sub, "validations_fatos_vs_staging")

        # --- Validação STAGING vs RAW (opcional) ---
        report = {
            "fonte": "db",
            "ano": int(yr),
            "tem_fato_despesa": not fd_y.empty,
            "tem_fato_receita": not fr_y.empty,
        }
        if args.rawdir:
            raw_sum = raw_totais_for_year(Path(args.rawdir), int(yr))
            raw_df = pd.DataFrame([{**{"ano": int(yr)}, **raw_sum}])
            # compara com STG
            if not val_stg_d.empty:
                for k in ("empenhado","liquidado","pago"):
                    raw_df[f"diff_raw_stg_{k}"] = raw_df[k] - float(val_stg_d.iloc[0].get(k, 0.0))
            if not val_stg_r.empty:
                for k in ("previsto","arrecadado"):
                    raw_df[f"diff_raw_stg_{k}"] = raw_df[k] - float(val_stg_r.iloc[0].get(k, 0.0))
            write(raw_df, sub, "validations_staging_vs_raw")
            report["has_raw"] = True
        else:
            report["has_raw"] = False

        # --- Integrar flags de quality/reconcile (se passadas em --qcdir) ---
        if args.qcdir:
            qcroot = Path(args.qcdir)
            # quality checks
            r1 = qcroot / "quality" / "R1_inequalities.csv"
            r6 = qcroot / "quality" / "R6_yoy_anomalias.csv"
            def count_year_csv(path: Path) -> int:
                if not path.exists(): return 0
                try:
                    t = pd.read_csv(path)
                    if any(c.lower()=="exercicio" for c in t.columns):
                        col = [c for c in t.columns if c.lower()=="exercicio"][0]
                        return int((t[col].astype(str)==str(yr)).sum())
                except Exception:
                    return 0
                return 0
            report["qc_r1_inequalities_rows"] = count_year_csv(r1)
            report["qc_r6_yoy_anomalias_rows"] = count_year_csv(r6)
            # reconcile
            drec = qcroot / "reconcile_raw_vs_portal" / "D_despesas_reconcile.csv"
            rrec = qcroot / "reconcile_raw_vs_portal" / "R_receita_reconcile.csv"
            def sum_year_csv(path: Path, colsum: str) -> float:
                if not path.exists(): return 0.0
                try:
                    t = pd.read_csv(path)
                    if any(c.lower()=="exercicio" for c in t.columns) and any(c.lower()==colsum for c in t.columns):
                        e = [c for c in t.columns if c.lower()=="exercicio"][0]
                        d = [c for c in t.columns if c.lower()==colsum][0]
                        t = t[t[e].astype(str)==str(yr)]
                        return float(pd.to_numeric(t[d], errors="coerce").fillna(0).abs().sum())
                except Exception:
                    return 0.0
                return 0.0
            report["reconcile_despesa_diff_abs_sum"] = sum_year_csv(drec, "diff_abs")
            report["reconcile_receita_diff_arrecadacao_sum"] = sum_year_csv(rrec, "diff_arrecadacao")

        # dump do relatório
        sub.mkdir(parents=True, exist_ok=True)
        (sub / "data_coverage_report.json").write_text(json.dumps(report, ensure_ascii=False, indent=2))
        print(f"📄 data_coverage_report → {sub}/data_coverage_report.json")

    print("✅ KPIs exportados e validados (Fatos↔Staging; opcional RAW/QC).")

if __name__ == "__main__":
    main()
