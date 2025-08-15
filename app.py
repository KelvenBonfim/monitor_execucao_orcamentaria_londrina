# app.py
from __future__ import annotations

import os
from pathlib import Path
from functools import lru_cache

import pandas as pd
import plotly.express as px
import streamlit as st
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

# -----------------------------------------------------------------------------
# Configuração de página
# -----------------------------------------------------------------------------
st.set_page_config(
    page_title="Monitor Execução Orçamentária - Londrina",
    page_icon="📊",
    layout="wide"
)

# =============== CSS responsivo (auto no mobile) ===============
st.markdown(
    """
    <style>
    @media (max-width: 768px) {
      .block-container { padding-left: 0.6rem; padding-right: 0.6rem; }
      h1, h2, h3 { font-size: 1.1rem; line-height: 1.2; }
      /* Métricas mais compactas */
      [data-testid="stMetricLabel"] { font-size: 0.8rem; }
      [data-testid="stMetricValue"] { font-size: 1.1rem; }
      /* Reduz expander padding */
      .streamlit-expanderHeader { font-size: 0.95rem; }
    }
    </style>
    """,
    unsafe_allow_html=True,
)

# -----------------------------------------------------------------------------
# Config — Fonte de dados (DB Neon OU CSVs fallback)
# -----------------------------------------------------------------------------
DATA_DIR = Path("data/kpis")  # suas saídas legadas (para despesa/serie)
# Aceita env var OU st.secrets
DB_URL = os.getenv("DATABASE_URL", st.secrets.get("DATABASE_URL", "")).strip()
USE_DB = bool(DB_URL)

# CSVs exportados das views (fallback)
CSV_VW_TIPO   = Path("raw/receitas/vw_receita_por_tipo.csv")
CSV_VW_RESUMO = Path("raw/receitas/vw_receita_resumo_anual.csv")

# -----------------------------------------------------------------------------
# Helpers visuais e utilitários
# -----------------------------------------------------------------------------
def br_money(x: float | int | None) -> str:
    if x is None:
        x = 0
    # 2 casas (mais adequado para receitas)
    return f"{float(x):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

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
        st.caption("Fonte de dados: **Neon PostgreSQL** (`DATABASE_URL`).")
    else:
        st.caption("Fonte de dados: **CSVs** — views de receita em `raw/receitas/vw_*.csv` e demais em `data/kpis/`.")

def plot_bar_fmt(fig, escala: str, compact: bool = False):
    fig.update_traces(
        texttemplate="%{text}",
        hovertemplate="<b>%{x}</b><br>" +
                      f"{label_valor(escala)}: %{{customdata}}<extra></extra>"
    )
    base_font = 13 if not compact else 11
    tick_angle = 45 if not compact else 0
    fig.update_layout(
        font=dict(size=base_font),
        uniformtext_minsize=8 if not compact else 6,
        uniformtext_mode="hide",
        margin=dict(l=8 if compact else 10, r=8 if compact else 10, t=26 if compact else 30, b=8 if compact else 10),
        xaxis_tickangle=tick_angle,
        legend=dict(orientation="h", yanchor="bottom", y=-0.25 if compact else -0.15, x=0)
    )
    return fig

def download_df_button(df: pd.DataFrame, filename: str, label: str):
    st.download_button(
        label=label,
        data=df.to_csv(index=False).encode("utf-8"),
        file_name=filename,
        mime="text/csv"
    )

# -----------------------------------------------------------------------------
# DB helpers (quando USE_DB = True)
# -----------------------------------------------------------------------------
@lru_cache(maxsize=1)
def get_engine():
    if not USE_DB:
        raise RuntimeError("DATABASE_URL não definido.")
    return create_engine(DB_URL, pool_pre_ping=True)

@st.cache_data(show_spinner=False)
def db_list_years() -> list[int]:
    """
    Pega anos preferencialmente da view de receita. Se falhar, tenta fato_despesa.
    """
    try:
        df = pd.read_sql(text("SELECT DISTINCT ano FROM public.vw_receita_resumo_anual ORDER BY ano;"), get_engine())
        years = df["ano"].astype(int).tolist()
        if years:
            return years
    except Exception:
        pass

    # fallback: fato_despesa
    try:
        df = pd.read_sql(text("SELECT DISTINCT exercicio AS ano FROM public.fato_despesa ORDER BY exercicio;"), get_engine())
        return df["ano"].astype(int).tolist()
    except Exception:
        return []

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
    """
    Traz totais de receita da view public.vw_receita_resumo_anual:
    colunas: exercicio, previsto, arrecadado
    """
    sql_view = """
      SELECT
        ano AS exercicio,
        previsao_total    AS previsto,
        arrecadacao_total AS arrecadado
      FROM public.vw_receita_resumo_anual
      ORDER BY ano;
    """
    return pd.read_sql(text(sql_view), get_engine())

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
def db_receita_por_tipo(ano: int) -> pd.DataFrame:
    """
    Nível categoria (sem subitens), diretamente da view public.vw_receita_por_tipo.
    Retorna colunas: codigo, tipo, previsto, arrecadado.
    """
    sql = """
      SELECT
        LPAD(codigo, 2, '0') AS codigo,
        TRIM(especificacao)  AS tipo,
        previsao             AS previsto,
        arrecadacao          AS arrecadado
      FROM public.vw_receita_por_tipo
      WHERE ano = :ano
      ORDER BY arrecadacao DESC, previsto DESC, codigo;
    """
    df = pd.read_sql(text(sql), get_engine(), params={"ano": int(ano)})
    for c in ["codigo", "tipo"]:
        df[c] = df[c].astype(str).str.strip()
    for c in ["previsto", "arrecadado"]:
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0.0)
    return df

# -----------------------------------------------------------------------------
# CSV fallback — views de receita
# -----------------------------------------------------------------------------
@st.cache_data(show_spinner=False)
def csvvw_totais_receita():
    if not CSV_VW_RESUMO.exists():
        return pd.DataFrame()
    df = pd.read_csv(CSV_VW_RESUMO)
    return df.rename(columns={
        "ano": "exercicio",
        "previsao_total": "previsto",
        "arrecadacao_total": "arrecadado"
    })

@st.cache_data(show_spinner=False)
def csvvw_receita_por_tipo(ano: int):
    if not CSV_VW_TIPO.exists():
        return pd.DataFrame()
    df = pd.read_csv(CSV_VW_TIPO)
    df = df[df["ano"] == int(ano)].copy()
    df = df.rename(columns={"especificacao": "tipo"})
    for c in ["previsao", "arrecadacao"]:
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0.0)
    df = df.rename(columns={"previsao": "previsto", "arrecadacao": "arrecadado"})
    return df[["codigo", "tipo", "previsto", "arrecadado"]].sort_values("arrecadado", ascending=False)

# -----------------------------------------------------------------------------
# CSV legado (para despesa/serie)
# -----------------------------------------------------------------------------
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
            if "ano" not in df.columns and "exercicio" in df.columns:
                df = df.rename(columns={"exercicio": "ano"})
            df["ano"] = int(y)
            out.append(df)
    return pd.concat(out, ignore_index=True) if out else pd.DataFrame()

# -----------------------------------------------------------------------------
# Sidebar — Filtros
# -----------------------------------------------------------------------------
years = db_list_years() if USE_DB else (
    sorted(pd.read_csv(CSV_VW_RESUMO)["ano"].unique().tolist()) if CSV_VW_RESUMO.exists() else fs_list_years()
)
if not years:
    st.error("Nenhum dado encontrado (DB/CSVs).")
    info_source()
    st.stop()

st.sidebar.header("⚙️ Filtros")
year = st.sidebar.selectbox("Ano", years, index=len(years) - 1)
anos_serie = st.sidebar.multiselect("Anos na série (evolução)", years, default=years)
escala = st.sidebar.radio("Escala dos valores", ["unidade", "mil", "milhões", "bilhões"], horizontal=True, index=2)
top_n = st.sidebar.slider("Top N", 5, 30, 15)
metrica_ent = st.sidebar.radio("Métrica para 'por Entidade'", ["pago", "liquidado", "empenhado"], index=0, horizontal=True)
busca_ent = st.sidebar.text_input("Filtro de entidade (contém)", value="").strip()
compact_mode = st.sidebar.checkbox("Modo compacto (mobile)", value=False, help="Reduz fontes, margens e ângulo do eixo X nos gráficos.")
show_legend_codigo = st.sidebar.checkbox("Mostrar legenda por código no 'Receita por Tipo'", value=False)

# -----------------------------------------------------------------------------
# Header
# -----------------------------------------------------------------------------
st.title("📊 Monitor de Execução Orçamentária — Londrina")
info_source()
st.markdown("---")

# -----------------------------------------------------------------------------
# Abas
# -----------------------------------------------------------------------------
tab_resumo, tab_despesa, tab_receita, tab_serie = st.tabs(
    ["📌 Resumo", "🏛️ Despesa por Entidade", "💰 Receita por Tipo", "📈 Série Anual"]
)

# -----------------------------------------------------------------------------
# Resumo (ano selecionado)
# -----------------------------------------------------------------------------
with tab_resumo:
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
    else:
        # Receita dos CSVs das views
        rec_view = csvvw_totais_receita()
        rec = rec_view[rec_view["exercicio"] == year] if not rec_view.empty else pd.DataFrame()
        prev = float(rec["previsto"].iloc[0]) if not rec.empty else 0.0
        arr = float(rec["arrecadado"].iloc[0]) if not rec.empty else 0.0

        # Despesa do seu repositório de KPIs
        glob = fs_load_csv(year, "execucao_global_anual")
        e = float(glob["empenhado"].iloc[0]) if not glob.empty else 0.0
        l = float(glob["liquidado"].iloc[0]) if not glob.empty else 0.0
        p = float(glob["pago"].iloc[0]) if not glob.empty else 0.0

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Empenhado", f"{br_money(scale_number(e, escala))} {label_valor(escala)}")
    c2.metric("Liquidado", f"{br_money(scale_number(l, escala))} {label_valor(escala)}")
    c3.metric("Pago",      f"{br_money(scale_number(p, escala))} {label_valor(escala)}")
    c4.metric("Receita Prevista",   f"{br_money(scale_number(prev, escala))} {label_valor(escala)}")
    c5.metric("Receita Arrecadada", f"{br_money(scale_number(arr,  escala))} {label_valor(escala)}",
              delta=f"{br_money(scale_number(arr - prev, escala))} {label_valor(escala)}")

# -----------------------------------------------------------------------------
# Despesa por Entidade (ano)
# -----------------------------------------------------------------------------
with tab_despesa:
    st.subheader("Despesa por Entidade — ano selecionado")
    if USE_DB:
        ent = db_despesa_por_entidade(year)
    else:
        ent = fs_load_csv(year, "execucao_por_entidade_anual")

    if not ent.empty:
        if "entidade" not in ent.columns:
            cand = [c for c in ent.columns if "entid" in c.lower()]
            if cand:
                ent = ent.rename(columns={cand[0]: "entidade"})

        if busca_ent:
            ent = ent[ent["entidade"].str.contains(busca_ent, case=False, na=False)]

        if metrica_ent not in ent.columns:
            st.warning(f"A métrica '{metrica_ent}' não está disponível nos dados.")
        else:
            ent_plot = ent[["entidade", metrica_ent]].copy()
            ent_plot[metrica_ent] = pd.to_numeric(ent_plot[metrica_ent], errors="coerce").fillna(0.0)
            ent_plot = ent_plot.sort_values(metrica_ent, ascending=False).head(top_n)
            ent_plot["valor_escala"] = ent_plot[metrica_ent].apply(lambda v: scale_number(v, escala))
            ent_plot["texto_barra"]  = ent_plot["valor_escala"].apply(lambda v: br_money(v))

            fig = px.bar(
                ent_plot,
                x="entidade",
                y="valor_escala",
                text="texto_barra",
                custom_data=[ent_plot[metrica_ent].apply(br_money)],
                labels={"valor_escala": label_valor(escala), "entidade": ""}
            )
            fig.update_xaxes(categoryorder="total descending")
            fig = plot_bar_fmt(fig, escala, compact=compact_mode)
            st.plotly_chart(fig, use_container_width=True, config={"responsive": True, "displayModeBar": False})

            with st.expander("Dados usados neste gráfico"):
                st.dataframe(
                    ent_plot[["entidade", metrica_ent]].rename(columns={metrica_ent: "valor (bruto)"}),
                    use_container_width=True,
                    height=300 if compact_mode else None
                )
                download_df_button(ent_plot[["entidade", metrica_ent]], f"despesa_por_entidade_{year}.csv", "Baixar CSV")
    else:
        st.info("Não há dados de despesa por entidade para o ano.")

# -----------------------------------------------------------------------------
# Receita por Tipo (ano)
# -----------------------------------------------------------------------------
with tab_receita:
    st.subheader("Receita por Tipo — ano selecionado")
    if USE_DB:
        rec_tipo = db_receita_por_tipo(year)
    else:
        # usa CSV da view se disponível; senão, deixa vazio
        rec_tipo = csvvw_receita_por_tipo(year)

    if not rec_tipo.empty:
        ycol = "arrecadado" if "arrecadado" in rec_tipo.columns else "previsto"
        rec_tipo = rec_tipo.copy()
        rec_tipo[ycol] = pd.to_numeric(rec_tipo[ycol], errors="coerce").fillna(0.0)
        rec_tipo = rec_tipo.sort_values(ycol, ascending=False).head(top_n)
        rec_tipo["valor_escala"] = rec_tipo[ycol].apply(lambda v: scale_number(v, escala))
        rec_tipo["texto_barra"]  = rec_tipo["valor_escala"].apply(lambda v: br_money(v))

        color_kw = {"color": "codigo"} if show_legend_codigo and "codigo" in rec_tipo.columns else {}

        fig = px.bar(
            rec_tipo,
            x="tipo",
            y="valor_escala",
            text="texto_barra",
            custom_data=[rec_tipo[ycol].apply(br_money), rec_tipo.get("codigo", pd.Series([""]*len(rec_tipo)))],
            labels={"valor_escala": label_valor(escala), "tipo": ""},
            **color_kw
        )
        fig.update_traces(
            hovertemplate="<b>%{x}</b>"
                          + ("<br>Código: %{{customdata[1]}}" if show_legend_codigo and "codigo" in rec_tipo.columns else "")
                          + "<br>" + f"{label_valor(escala)}: %{{customdata[0]}}<extra></extra>"
        )
        fig.update_xaxes(categoryorder="total descending")
        fig = plot_bar_fmt(fig, escala, compact=compact_mode)
        st.plotly_chart(fig, use_container_width=True, config={"responsive": True, "displayModeBar": False})

        with st.expander("Dados usados neste gráfico"):
            cols = ["codigo", "tipo", ycol] if "codigo" in rec_tipo.columns else ["tipo", ycol]
            st.dataframe(
                rec_tipo[cols].rename(columns={ycol: "valor (bruto)"}),
                use_container_width=True,
                height=300 if compact_mode else None
            )
            download_df_button(rec_tipo[cols], f"receita_por_tipo_{year}.csv", "Baixar CSV")
    else:
        st.info("Sem dados de receita por tipo para o ano.")

# -----------------------------------------------------------------------------
# Evolução anual — série
# -----------------------------------------------------------------------------
with tab_serie:
    st.subheader("Evolução anual — Empenhado, Liquidado, Pago")
    if USE_DB:
        serie_glob = db_totais_despesa().rename(columns={"exercicio": "ano"})
    else:
        serie_glob = fs_load_series("execucao_global_anual")

    if not serie_glob.empty:
        serie_glob = serie_glob[serie_glob["ano"].isin(anos_serie)].copy()
        for col in ["empenhado", "liquidado", "pago"]:
            if col in serie_glob.columns:
                serie_glob[col] = pd.to_numeric(serie_glob[col], errors="coerce").fillna(0.0)
                serie_glob[col] = serie_glob[col].apply(lambda v: scale_number(v, escala))
        show_vals = st.checkbox("Mostrar valores na linha", value=False, key="lbl_serie")
        fig = px.line(
            serie_glob.sort_values("ano"),
            x="ano",
            y=["empenhado", "liquidado", "pago"],
            markers=True,
            labels={"value": label_valor(escala), "variable": "Estágio"}
        )
        if show_vals:
            fig.update_traces(mode="lines+markers+text", textposition="top center", texttemplate="%{y:.2s}")
        fig.update_layout(
            font=dict(size=13 if not compact_mode else 11),
            margin=dict(l=8 if compact_mode else 10, r=8 if compact_mode else 10,
                        t=26 if compact_mode else 30, b=8 if compact_mode else 10),
            legend=dict(orientation="h", yanchor="bottom", y=-0.25 if compact_mode else -0.15, x=0)
        )
        st.plotly_chart(fig, use_container_width=True, config={"responsive": True, "displayModeBar": False})
    else:
        st.info("Sem série anual consolidada.")

st.markdown("---")
st.caption("Modo DB usa *views* (`vw_receita_resumo_anual`, `vw_receita_por_tipo`). Modo CSV usa exports dessas views e KPIs legados em `data/kpis/`.")
