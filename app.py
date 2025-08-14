# app.py
from pathlib import Path
import pandas as pd
import plotly.express as px
import streamlit as st

st.set_page_config(
    page_title="Monitor Execução Orçamentária - Londrina",
    page_icon="📊",
    layout="wide"
)

# =========================================
# Fonte de dados (DB Neon OU CSVs fallback)
# =========================================
DATA_DIR = Path("data/kpis")
USE_DB = bool(st.secrets.get("DATABASE_URL", "").strip())

# ---------------- helpers ----------------
def br_money(x: float | int | None) -> str:
    if x is None:
        x = 0
    return f"{float(x):,.0f}".replace(",", "X").replace(".", ",").replace("X", ".")

def scale_number(x: float, escala: str) -> float:
    if escala == "mil":
        return x / 1_000
    if escala == "milhões":
        return x / 1_000_000
    if escala == "bilhões":
        return x / 1_000_000_000
    return x

def label_valor(escala: str) -> str:
    return {"unidade":"R$", "mil":"R$ mil", "milhões":"R$ mi", "bilhões":"R$ bi"}[escala]

def info_source():
    if USE_DB:
        st.caption("Fonte de dados: **Neon PostgreSQL** (`st.secrets['DATABASE_URL']`).")
    else:
        st.caption("Fonte de dados: **CSVs** em `data/kpis/` (fallback).")

# =========================
# Carregamento via DB (Neon)
# =========================
if USE_DB:
    from sqlalchemy import create_engine, text

    @st.cache_resource(show_spinner=False)
    def get_engine():
        return create_engine(st.secrets["DATABASE_URL"])

    @st.cache_data(show_spinner=False)
    def db_list_years() -> list[int]:
        sql = """
          WITH y1 AS (SELECT DISTINCT exercicio AS ano FROM public.fato_despesa),
               y2 AS (SELECT DISTINCT exercicio AS ano FROM public.fato_receita)
          SELECT DISTINCT ano FROM (SELECT ano FROM y1 UNION SELECT ano FROM y2) t
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
        """Tenta dois esquemas de nomes de colunas e normaliza para previsto/arrecadado."""
        eng = get_engine()
        sql_v1 = """
          SELECT exercicio,
                 SUM(valor_previsto)   AS previsto,
                 SUM(valor_arrecadado) AS arrecadado
          FROM public.fato_receita
          GROUP BY exercicio
          ORDER BY exercicio;
        """
        try:
            return pd.read_sql(text(sql_v1), eng)
        except Exception:
            sql_v2 = """
              SELECT exercicio,
                     SUM(previsao)    AS previsto,
                     SUM(arrecadacao) AS arrecadado
              FROM public.fato_receita
              GROUP BY exercicio
              ORDER BY exercicio;
            """
            return pd.read_sql(text(sql_v2), eng)

    @st.cache_data(show_spinner=False)
    def db_despesa_por_entidade(ano: int):
        sql = """
          SELECT entidade,
                 SUM(valor_empenhado) AS empenhado,
                 SUM(valor_liquidado) AS liquidado,
                 SUM(valor_pago)      AS pago
          FROM public.fato_despesa
          WHERE exercicio = :ano
          GROUP BY entidade
          ORDER BY pago DESC;
        """
        return pd.read_sql(text(sql), get_engine(), params={"ano": int(ano)})

    @st.cache_data(show_spinner=False)
    def db_receita_por_codigo(ano: int) -> pd.DataFrame:
        """
        Por código (tipo de receita). Usa a especificação não nula mais longa por código.
        Funciona com esquemas (previsao/arrecadacao) OU (valor_previsto/valor_arrecadado).
        """
        eng = get_engine()
        # Detecta colunas de valores
        try:
            pd.read_sql(text("SELECT previsao, arrecadacao FROM public.fato_receita LIMIT 1"), eng)
            prev_col = "previsao"
            arr_col  = "arrecadacao"
        except Exception:
            prev_col = "valor_previsto"
            arr_col  = "valor_arrecadado"

        sql = f"""
          SELECT
            TRIM(COALESCE(codigo::text, '')) AS codigo,
            COALESCE(
              (array_agg(NULLIF(btrim(especificacao), '') ORDER BY length(btrim(especificacao)) DESC))[1],
              ''
            ) AS especificacao,
            SUM(COALESCE({prev_col}, 0)) AS previsto,
            SUM(COALESCE({arr_col},  0)) AS arrecadado
          FROM public.fato_receita
          WHERE exercicio = :ano
          GROUP BY 1
          HAVING TRIM(COALESCE(codigo::text, '')) <> ''
          ORDER BY arrecadado DESC;
        """
        df = pd.read_sql(text(sql), eng, params={"ano": int(ano)})
        # limpeza final
        for c in ["codigo", "especificacao"]:
            if c in df.columns:
                df[c] = df[c].astype(str).str.strip()
        return df

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
    out = []
    for y in fs_list_years():
        df = fs_load_csv(y, name)
        if not df.empty:
            # normaliza nome do ano
            if "ano" not in df.columns and "exercicio" in df.columns:
                df = df.rename(columns={"exercicio": "ano"})
            df["ano"] = int(y)
            out.append(df)
    return pd.concat(out, ignore_index=True) if out else pd.DataFrame()

# =========================
# Sidebar — Filtros
# =========================
years = db_list_years() if USE_DB else fs_list_years()
if not years:
    st.error("Nenhum dado encontrado (DB vazio ou pasta data/kpis/ ausente).")
    info_source()
    st.stop()

st.sidebar.header("⚙️ Filtros")
year = st.sidebar.selectbox("Ano", years, index=len(years) - 1)
anos_serie = st.sidebar.multiselect("Anos na série (evolução)", years, default=years)
escala = st.sidebar.radio("Escala dos valores", ["unidade", "mil", "milhões", "bilhões"], horizontal=True, index=2)
top_n = st.sidebar.slider("Top N (Entidades/Códigos)", 5, 30, 15)
metrica_ent = st.sidebar.radio("Métrica para 'por Entidade'", ["pago", "liquidado", "empenhado"], index=0, horizontal=True)
busca_ent = st.sidebar.text_input("Filtro de entidade (contém)", value="").strip()

st.title("📊 Monitor de Execução Orçamentária — Londrina")
info_source()
st.markdown("---")

# =========================
# Resumo (ano selecionado)
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
    arr = float(rec["arrecadado"].iloc[0]) if not rec.empty else 0.0
    gap = prev - arr
else:
    glob = fs_load_csv(year, "execucao_global_anual")
    rec = fs_load_csv(year, "receita_prevista_arrecadada_anual")
    e = float(glob["empenhado"].iloc[0]) if not glob.empty else 0.0
    l = float(glob["liquidado"].iloc[0]) if not glob.empty else 0.0
    p = float(glob["pago"].iloc[0]) if not glob.empty else 0.0
    prev = float(rec["previsto"].iloc[0]) if not rec.empty else 0.0
    arr = float(rec["arrecadado"].iloc[0]) if not rec.empty else 0.0
    gap = float(rec["gap"].iloc[0]) if not rec.empty and "gap" in rec.columns else (prev - arr)

c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("Empenhado", f"{br_money(scale_number(e, escala))} {label_valor(escala)}")
c2.metric("Liquidado", f"{br_money(scale_number(l, escala))} {label_valor(escala)}")
c3.metric("Pago",      f"{br_money(scale_number(p, escala))} {label_valor(escala)}")
c4.metric("Receita Prevista",   f"{br_money(scale_number(prev, escala))} {label_valor(escala)}")
c5.metric("Receita Arrecadada", f"{br_money(scale_number(arr,  escala))} {label_valor(escala)}",
          delta=f"{br_money(scale_number(arr - prev, escala))} {label_valor(escala)}")

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
    serie_glob = serie_glob[serie_glob["ano"].isin(anos_serie)].copy()
    # aplicar escala
    for col in ["empenhado", "liquidado", "pago"]:
        if col in serie_glob.columns:
            serie_glob[col] = serie_glob[col].astype(float).apply(lambda v: scale_number(v, escala))
    fig = px.line(
        serie_glob.sort_values("ano"),
        x="ano",
        y=["empenhado", "liquidado", "pago"],
        markers=True,
        labels={"value": label_valor(escala), "variable": "Estágio"}
    )
    st.plotly_chart(fig, use_container_width=True)
else:
    st.info("Sem série anual consolidada.")

# =========================
# Receita — Prevista x Arrecadada (ano)
# =========================
st.subheader("Receita — Prevista x Arrecadada (ano selecionado)")
if USE_DB:
    df_r_all = db_totais_receita().rename(columns={"exercicio": "ano"})
    rec_ano = df_r_all[df_r_all["ano"] == year]
else:
    rec_ano = fs_load_csv(year, "receita_prevista_arrecadada_anual")

if not rec_ano.empty:
    rec_plot = rec_ano.copy()
    for c in ["previsto", "arrecadado"]:
        rec_plot[c] = rec_plot[c].astype(float).apply(lambda v: scale_number(v, escala))
    df_melt = rec_plot.melt(id_vars=["ano"], value_vars=["previsto", "arrecadado"],
                            var_name="tipo", value_name="valor")
    fig = px.bar(df_melt, x="tipo", y="valor", text_auto=".2s",
                 labels={"valor": label_valor(escala), "tipo": ""})
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
    ent = fs_load_csv(year, "execucao_por_entidade_anual")

if not ent.empty:
    # normalização
    if "entidade" not in ent.columns:
        cand = [c for c in ent.columns if "entid" in c.lower()]
        if cand:
            ent = ent.rename(columns={cand[0]: "entidade"})
    # filtro de busca
    if busca_ent:
        ent = ent[ent["entidade"].str.contains(busca_ent, case=False, na=False)]
    # metrica
    if metrica_ent not in ent.columns:
        st.warning(f"A métrica '{metrica_ent}' não está disponível nos dados.")
    else:
        ent_plot = ent[["entidade", metrica_ent]].copy()
        ent_plot[metrica_ent] = ent_plot[metrica_ent].astype(float).apply(lambda v: scale_number(v, escala))
        ent_plot = ent_plot.sort_values(metrica_ent, ascending=False).head(top_n)
        fig = px.bar(ent_plot, x="entidade", y=metrica_ent, text_auto=".2s",
                     labels={metrica_ent: label_valor(escala)})
        fig.update_xaxes(tickangle=45)
        st.plotly_chart(fig, use_container_width=True)
else:
    st.info("Não há dados de despesa por entidade para o ano.")

# =========================
# Receita por Código (ano)
# =========================
st.subheader("Receita por Código (ano selecionado)")
if USE_DB:
    rec_cod = db_receita_por_codigo(year)
else:
    rec_cod = fs_load_csv(year, "receita_por_codigo_anual")
    if not rec_cod.empty:
        # limpeza: tira espaços/linhas nulas e garante especificação não nula por código
        for c in rec_cod.columns:
            if rec_cod[c].dtype == object:
                rec_cod[c] = rec_cod[c].astype(str).str.strip()
        rec_cod = rec_cod[rec_cod["codigo"].astype(str).str.strip() != ""].copy()
        rec_cod["especificacao"] = rec_cod["especificacao"].fillna("").astype(str).str.strip()
        spec_fill = (
            rec_cod.loc[rec_cod["especificacao"] != ""]
                  .assign(len_spec=lambda d: d["especificacao"].str.len())
                  .sort_values(["codigo", "len_spec"], ascending=[True, False])
                  .drop_duplicates("codigo")[["codigo", "especificacao"]]
        )
        rec_cod = rec_cod.drop(columns=["especificacao"], errors="ignore") \
                         .merge(spec_fill, on="codigo", how="left")

# plot
if not rec_cod.empty:
    plot = rec_cod.copy()
    if "arrecadado" in plot.columns:
        plot["arrecadado"] = plot["arrecadado"].astype(float).apply(lambda v: scale_number(v, escala))
        ycol = "arrecadado"
    else:
        plot["previsto"] = plot["previsto"].astype(float).apply(lambda v: scale_number(v, escala))
        ycol = "previsto"

    plot = plot.sort_values(ycol, ascending=False).head(top_n)
    fig = px.bar(
        plot,
        x="codigo",
        y=ycol,
        text_auto=".2s",
        labels={ycol: label_valor(escala), "codigo": "Código"},
        hover_data={"especificacao": True, "codigo": True, ycol: True}
    )
    fig.update_xaxes(tickangle=45)
    st.plotly_chart(fig, use_container_width=True)
else:
    st.info("Sem dados de receita por código para o ano.")

st.markdown("---")
st.caption("No modo CSV, gere KPIs com `scripts/09_export_kpis.py` e faça commit em `data/kpis/`. No modo DB, os dados vêm direto do Neon.")
