# app.py
import os
from pathlib import Path
import pandas as pd
import plotly.express as px
import streamlit as st

st.set_page_config(
    page_title="Monitor Execução Orçamentária - Londrina",
    page_icon="📊",
    layout="wide"
)

# =========================
# Fonte de dados (DB Neon OU CSVs)
# =========================
DATA_DIR = Path("data/kpis")
USE_DB = bool(st.secrets.get("DATABASE_URL", "").strip())

# -------- helpers visuais --------
def br_money(x: float | int | None) -> str:
    if x is None:
        x = 0
    return f"{float(x):,.0f}".replace(",", "X").replace(".", ",").replace("X", ".")

def info_source():
    if USE_DB:
        st.caption("Fonte de dados: **Neon PostgreSQL** (st.secrets['DATABASE_URL'])")
    else:
        st.caption("Fonte de dados: **CSVs** em `data/kpis/` (fallback)")

# =========================
# Carregamento via DB (Neon)
# =========================
if USE_DB:
    from sqlalchemy import create_engine, text

    @st.cache_resource(show_spinner=False)
    def get_engine():
        # exige SQLAlchemy + psycopg2-binary
        return create_engine(st.secrets["DATABASE_URL"])

    @st.cache_data(show_spinner=False)
    def db_list_years() -> list[int]:
        sql = """
          WITH y1 AS (SELECT DISTINCT exercicio AS ano FROM public.fato_despesa),
               y2 AS (SELECT DISTINCT exercicio AS ano FROM public.fato_receita)
          SELECT DISTINCT ano FROM (
            SELECT ano FROM y1 UNION SELECT ano FROM y2
          ) t
          ORDER BY ano;
        """
        df = pd.read_sql(text(sql), get_engine())
        return df["ano"].astype(int).tolist()

    @st.cache_data(show_spinner=False)
    def db_totais_despesa():
        sql = """
          SELECT exercicio,
                 SUM(valor_empenhado) AS empenhado,
                 SUM(valor_liquidado) AS liquidado,
                 SUM(valor_pago)      AS pago
          FROM public.fato_despesa
          GROUP BY exercicio
          ORDER BY exercicio;
        """
        return pd.read_sql(text(sql), get_engine())

    @st.cache_data(show_spinner=False)
    def db_totais_receita():
        # supondo colunas valor_previsto / valor_arrecadado em fato_receita
        sql = """
          SELECT exercicio,
                 SUM(valor_previsto)   AS previsto,
                 SUM(valor_arrecadado) AS arrecadado
          FROM public.fato_receita
          GROUP BY exercicio
          ORDER BY exercicio;
        """
        return pd.read_sql(text(sql), get_engine())

    @st.cache_data(show_spinner=False)
    def db_despesa_por_entidade(ano: int):
        sql = """
          SELECT exercicio, entidade,
                 SUM(valor_empenhado) AS empenhado,
                 SUM(valor_liquidado) AS liquidado,
                 SUM(valor_pago)      AS pago
          FROM public.fato_despesa
          WHERE exercicio = :ano
          GROUP BY exercicio, entidade
          ORDER BY pago DESC;
        """
        return pd.read_sql(text(sql), get_engine(), params={"ano": int(ano)})

# =========================
# Carregamento via CSV (fallback)
# =========================
@st.cache_data(show_spinner=False)
def fs_list_years() -> list[int]:
    if not DATA_DIR.exists():
        return []
    years = [p.name for p in DATA_DIR.iterdir() if p.is_dir() and p.name.isdigit()]
    return sorted(map(int, years))

@st.cache_data(show_spinner=False)
def fs_load_csv(year: int, name: str) -> pd.DataFrame:
    p = DATA_DIR / f"{year}" / f"{name}.csv"
    if not p.exists():
        return pd.DataFrame()
    return pd.read_csv(p)

@st.cache_data(show_spinner=False)
def fs_load_series(name: str) -> pd.DataFrame:
    """Carrega o mesmo KPI de todos os anos (para séries de tendência)."""
    out = []
    for y in fs_list_years():
        df = fs_load_csv(y, name)
        if not df.empty:
            out.append(df.assign(ano=int(y)))
    return pd.concat(out, ignore_index=True) if out else pd.DataFrame()

# =========================
# Escolha do ano
# =========================
if USE_DB:
    years = db_list_years()
else:
    years = fs_list_years()

if not years:
    st.error("Nenhum dado encontrado (DB vazio ou pasta data/kpis/ ausente).")
    info_source()
    st.stop()

st.sidebar.header("⚙️ Filtros")
year = st.sidebar.selectbox("Ano", years, index=len(years) - 1)

st.title("📊 Monitor de Execução Orçamentária — Londrina")
info_source()
st.markdown("---")

# =========================
# Cards de resumo (ano selecionado)
# =========================
if USE_DB:
    df_d_all = db_totais_despesa()
    df_r_all = db_totais_receita()

    glob = df_d_all[df_d_all["exercicio"] == year]
    rec = df_r_all[df_r_all["exercicio"] == year]

    e = float(glob["empenhado"].iloc[0]) if not glob.empty else 0.0
    l = float(glob["liquidado"].iloc[0]) if not glob.empty else 0.0
    p = float(glob["pago"].iloc[0]) if not glob.empty else 0.0

    prev = float(rec["previsto"].iloc[0]) if not rec.empty else 0.0
    arr  = float(rec["arrecadado"].iloc[0]) if not rec.empty else 0.0
    gap  = prev - arr
else:
    glob = fs_load_csv(year, "execucao_global_anual")
    rec  = fs_load_csv(year, "receita_prevista_arrecadada_anual")

    e = float(glob["empenhado"].iloc[0]) if not glob.empty else 0.0
    l = float(glob["liquidado"].iloc[0]) if not glob.empty else 0.0
    p = float(glob["pago"].iloc[0]) if not glob.empty else 0.0

    prev = float(rec["previsto"].iloc[0]) if not rec.empty else 0.0
    arr  = float(rec["arrecadado"].iloc[0]) if not rec.empty else 0.0
    gap  = float(rec["gap"].iloc[0]) if not rec.empty and "gap" in rec.columns else (prev - arr)

c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("Empenhado (R$)", br_money(e))
c2.metric("Liquidado (R$)", br_money(l))
c3.metric("Pago (R$)",      br_money(p))
c4.metric("Receita Prevista (R$)",   br_money(prev))
c5.metric("Receita Arrecadada (R$)", br_money(arr), delta=br_money(arr - prev))

st.markdown("---")

# =========================
# Evolução anual — série
# =========================
st.subheader("Evolução anual — Empenhado, Liquidado, Pago")

if USE_DB:
    serie_glob = db_totais_despesa().rename(columns={"exercicio": "ano"})
else:
    serie_glob = fs_load_series("execucao_global_anual")

if not serie_glob.empty:
    # normalizar nome da coluna "ano"
    if "exercicio" in serie_glob.columns and "ano" not in serie_glob.columns:
        serie_glob = serie_glob.rename(columns={"exercicio": "ano"})
    fig = px.line(
        serie_glob.sort_values("ano"),
        x="ano",
        y=["empenhado", "liquidado", "pago"],
        markers=True,
        labels={"value": "R$", "variable": "Estágio"}
    )
    st.plotly_chart(fig, use_container_width=True)
else:
    st.info("Sem série anual consolidada.")

# =========================
# Receita: Prevista x Arrecadada (ano)
# =========================
st.subheader("Receita — Prevista x Arrecadada (ano selecionado)")

if USE_DB:
    df_r_all = db_totais_receita().rename(columns={"exercicio": "ano"})
    rec = df_r_all[df_r_all["ano"] == year]
    if not rec.empty:
        df_melt = rec.melt(id_vars=["ano"], value_vars=["previsto", "arrecadado"],
                           var_name="tipo", value_name="valor")
    else:
        df_melt = pd.DataFrame()
else:
    rec = fs_load_csv(year, "receita_prevista_arrecadada_anual")
    df_melt = rec.melt(id_vars=["ano"], value_vars=["previsto", "arrecadado"],
                       var_name="tipo", value_name="valor") if not rec.empty else pd.DataFrame()

if not df_melt.empty:
    fig = px.bar(df_melt, x="tipo", y="valor", text_auto=".2s", labels={"valor": "R$", "tipo": ""})
    st.plotly_chart(fig, use_container_width=True)
else:
    st.info("Sem dados de receita para o ano selecionado.")

# =========================
# Despesa por Entidade (ano)
# =========================
st.subheader("Despesa por Entidade (ano selecionado)")
if USE_DB:
    ent = db_despesa_por_entidade(year)
else:
    ent = fs_load_csv(year, "execucao_por_entidade_anual")  # disponível no modo CSV

if not ent.empty and "pago" in ent.columns:
    top_ent = ent.sort_values("pago", ascending=False).head(15).copy()
    # normalizar nome da coluna 'entidade' caso venha diferente
    if "entidade" not in top_ent.columns:
        # tenta encontrar uma coluna parecida
        cand = [c for c in top_ent.columns if "entid" in c.lower()]
        if cand:
            top_ent = top_ent.rename(columns={cand[0]: "entidade"})
    fig = px.bar(top_ent, x="entidade", y="pago", text_auto=".2s", labels={"pago": "Pago (R$)"})
    fig.update_xaxes(tickangle=45)
    st.plotly_chart(fig, use_container_width=True)
else:
    st.info("Não há dados de despesa por entidade para o ano (no modo DB só há `entidade`; 'função' e 'órgão/unidade' exigem KPIs CSV).")

# =========================
# Seções avançadas (apenas se CSVs existem)
# =========================
st.markdown("---")
st.subheader("Seções adicionais (se disponível nos CSVs)")

# Por Função
fun = fs_load_csv(year, "execucao_por_funcao_anual")
if not fun.empty and "pago" in fun.columns:
    top_fun = fun.sort_values("pago", ascending=False).head(15)
    col1, col2 = st.columns([2, 1])
    with col1:
        fig = px.bar(top_fun, x="funcao", y="pago", text_auto=".2s", labels={"pago": "Pago (R$)"})
        fig.update_xaxes(tickangle=45)
        st.plotly_chart(fig, use_container_width=True)
    with col2:
        if "pago_share" in fun.columns:
            pie = top_fun.copy()
            pie["share_%"] = pie["pago_share"] * 100
            fig2 = px.pie(pie, names="funcao", values="share_%", title="Participação no total (Top 15)")
            st.plotly_chart(fig2, use_container_width=True)
else:
    st.caption("ℹ️ KPI `execucao_por_funcao_anual.csv` não encontrado em `data/kpis/<ano>/`.")

# Por Órgão/Unidade
ou = fs_load_csv(year, "execucao_por_orgao_unidade_anual")
if not ou.empty and {"orgao", "unidade", "pago"}.issubset(ou.columns):
    top_ou = ou.sort_values("pago", ascending=False).head(15).copy()
    top_ou["orgao_unidade"] = top_ou["orgao"].astype(str) + " — " + top_ou["unidade"].astype(str)
    fig = px.bar(top_ou, x="orgao_unidade", y="pago", text_auto=".2s", labels={"pago": "Pago (R$)"})
    fig.update_xaxes(tickangle=45)
    st.plotly_chart(fig, use_container_width=True)
else:
    st.caption("ℹ️ KPI `execucao_por_orgao_unidade_anual.csv` não encontrado em `data/kpis/<ano>/`.")

st.markdown("---")
st.caption("Dica: no modo CSV, gere novos KPIs com `scripts/09_export_kpis.py` e faça commit em `data/kpis/`. No modo DB, os dados vêm direto do Neon.")