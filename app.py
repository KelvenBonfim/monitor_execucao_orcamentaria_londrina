import streamlit as st
import pandas as pd
from pathlib import Path
import plotly.express as px

st.set_page_config(page_title="Monitor Execução Orçamentária - Londrina",
                   page_icon="📊",
                   layout="wide")

DATA_DIR = Path("data/kpis")

@st.cache_data(show_spinner=False)
def list_years():
    if not DATA_DIR.exists():
        return []
    years = [p.name for p in DATA_DIR.iterdir() if p.is_dir() and p.name.isdigit()]
    return sorted(map(int, years))

@st.cache_data(show_spinner=False)
def load_csv(year: int, name: str) -> pd.DataFrame:
    p = DATA_DIR / f"{year}" / f"{name}.csv"
    if not p.exists():
        return pd.DataFrame()
    return pd.read_csv(p)

@st.cache_data(show_spinner=False)
def load_series(name: str) -> pd.DataFrame:
    """Carrega o mesmo KPI de todos os anos (para linhas de tendência)."""
    out = []
    for y in list_years():
        df = load_csv(y, name)
        if not df.empty:
            out.append(df.assign(_ano_ref=int(y)))
    return pd.concat(out, ignore_index=True) if out else pd.DataFrame()

years = list_years()
if not years:
    st.error("Nenhum dado encontrado em data/kpis/. Suba os CSVs gerados pelo 09_export_kpis.py.")
    st.stop()

st.sidebar.header("⚙️ Filtros")
year = st.sidebar.selectbox("Ano", years, index=len(years)-1)

st.title("📊 Monitor de Execução Orçamentária — Londrina")
st.caption("Fonte: fatos e KPIs gerados pelo pipeline local")

# ================== Cards de resumo (ano selecionado) ==================
glob = load_csv(year, "execucao_global_anual")
rec  = load_csv(year, "receita_prevista_arrecadada_anual")
if not glob.empty:
    e = float(glob["empenhado"].iloc[0])
    l = float(glob["liquidado"].iloc[0])
    p = float(glob["pago"].iloc[0])
else:
    e = l = p = 0.0
if not rec.empty:
    prev = float(rec["previsto"].iloc[0]); arr = float(rec["arrecadado"].iloc[0]); gap = float(rec["gap"].iloc[0])
else:
    prev = arr = gap = 0.0

c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("Empenhado (R$)", f"{e:,.0f}".replace(",", "X").replace(".", ",").replace("X", "."))
c2.metric("Liquidado (R$)", f"{l:,.0f}".replace(",", "X").replace(".", ",").replace("X", "."))
c3.metric("Pago (R$)", f"{p:,.0f}".replace(",", "X").replace(".", ",").replace("X", "."))
c4.metric("Receita Prevista (R$)", f"{prev:,.0f}".replace(",", "X").replace(".", ",").replace("X", "."))
c5.metric("Arrecadada (R$)", f"{arr:,.0f}".replace(",", "X").replace(".", ",").replace("X", "."), delta=f"{-gap:,.0f}".replace(",", "X").replace(".", ",").replace("X", "."))

st.markdown("---")

# ================== Evolução (todos os anos) ==================
st.subheader("Evolução anual — Empenhado, Liquidado, Pago")
serie_glob = load_series("execucao_global_anual")
if not serie_glob.empty:
    # quando juntamos todos os anos, já temos coluna 'ano'
    fig = px.line(serie_glob.sort_values("ano"), x="ano", y=["empenhado","liquidado","pago"],
                  markers=True, labels={"value":"R$", "variable":"Estágio"})
    st.plotly_chart(fig, use_container_width=True)
else:
    st.info("Sem série anual consolidada.")

# ================== Receita prevista x arrecadada (ano) ==================
st.subheader("Receita: Prevista x Arrecadada (ano selecionado)")
if not rec.empty:
    df_melt = rec.melt(id_vars=["ano"], value_vars=["previsto","arrecadado"], var_name="tipo", value_name="valor")
    fig = px.bar(df_melt, x="tipo", y="valor", text_auto=".2s", labels={"valor":"R$", "tipo":""})
    st.plotly_chart(fig, use_container_width=True)
else:
    st.info("Arquivo receita_prevista_arrecadada_anual.csv não encontrado para o ano.")

# ================== Por Função ==================
st.subheader("Despesa por Função (ano selecionado)")
fun = load_csv(year, "execucao_por_funcao_anual")
if not fun.empty:
    top_fun = fun.sort_values("pago", ascending=False).head(15)
    col1, col2 = st.columns([2,1])
    with col1:
        fig = px.bar(top_fun, x="funcao", y="pago", text_auto=".2s", labels={"pago":"Pago (R$)"})
        fig.update_xaxes(tickangle=45)
        st.plotly_chart(fig, use_container_width=True)
    with col2:
        if "pago_share" in fun.columns:
            pie = top_fun.copy()
            pie["share_%"] = pie["pago_share"]*100
            fig2 = px.pie(pie, names="funcao", values="share_%", title="Participação no total (Top 15)")
            st.plotly_chart(fig2, use_container_width=True)
else:
    st.info("Sem arquivo execucao_por_funcao_anual.csv para o ano.")

# ================== Por Entidade ==================
st.subheader("Despesa por Entidade (ano selecionado)")
ent = load_csv(year, "execucao_por_entidade_anual")
if not ent.empty:
    top_ent = ent.sort_values("pago", ascending=False).head(15)
    fig = px.bar(top_ent, x="entidade", y="pago", text_auto=".2s", labels={"pago":"Pago (R$)"})
    fig.update_xaxes(tickangle=45)
    st.plotly_chart(fig, use_container_width=True)
else:
    st.info("Sem arquivo execucao_por_entidade_anual.csv para o ano (ou coluna 'entidade' não existe nos Fatos).")

# ================== Por Órgão / Unidade ==================
st.subheader("Despesa por Órgão / Unidade (ano selecionado)")
ou = load_csv(year, "execucao_por_orgao_unidade_anual")
if not ou.empty:
    top_ou = ou.sort_values("pago", ascending=False).head(15)
    top_ou["orgao_unidade"] = top_ou["orgao"].astype(str) + " — " + top_ou["unidade"].astype(str)
    fig = px.bar(top_ou, x="orgao_unidade", y="pago", text_auto=".2s", labels={"pago":"Pago (R$)"})
    fig.update_xaxes(tickangle=45)
    st.plotly_chart(fig, use_container_width=True)
else:
    st.info("Sem arquivo execucao_por_orgao_unidade_anual.csv para o ano.")

st.markdown("---")
st.caption("Dica: para atualizar o dashboard, gere novos CSVs localmente e faça commit na pasta data/kpis do repositório.")