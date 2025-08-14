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

def plot_bar_fmt(fig, ycol, escala):
    fig.update_traces(
        texttemplate="%{text}",
        hovertemplate="<b>%{x}</b><br>" +
                      f"{label_valor(escala)}: %{customdata}<extra></extra>"
    )
    fig.update_layout(
        uniformtext_minsize=8, uniformtext_mode="hide",
        margin=dict(l=10, r=10, t=30, b=10),
        xaxis_tickangle=45
    )
    return fig

def download_df_button(df: pd.DataFrame, filename: str, label: str):
    st.download_button(
        label=label,
        data=df.to_csv(index=False).encode("utf-8"),
        file_name=filename,
        mime="text/csv"
    )

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
        """Tenta dois esquemas e normaliza para previsto/arrecadado."""
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
    def db_receita_por_tipo(ano: int) -> pd.DataFrame:
        """
        Por tipo (especificação). Usa a especificação não nula mais longa por código.
        Funciona com (previsao/arrecadacao) OU (valor_previsto/valor_arrecadado).
        """
        eng = get_engine()

        # Detecta colunas de valores
        try:
            pd.read_sql(text("SELECT previsao, arrecadacao FROM public.fato_receita LIMIT 1"), eng)
            prev_col = "previsao"
            arr_col = "arrecadacao"
        except Exception:
            prev_col = "valor_previsto"
            arr_col  = "valor_arrecadado"

        sql = f"""
          SELECT
            TRIM(COALESCE(codigo::text, '')) AS codigo,
            COALESCE(
              (array_agg(NULLIF(btrim(especificacao), '') ORDER BY length(btrim(especificacao)) DESC))[1],
              ''
            ) AS tipo,
            SUM(COALESCE({prev_col}, 0)) AS previsto,
            SUM(COALESCE({arr_col},  0)) AS arrecadado
          FROM public.fato_receita
          WHERE exercicio = :ano
          GROUP BY 1
          HAVING TRIM(COALESCE(codigo::text, '')) <> ''
          ORDER BY arrecadado DESC;
        """
        df = pd.read_sql(text(sql), eng, params={"ano": int(ano)})
        for c in ["codigo", "tipo"]:
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
            if "ano" not in df.columns and "exercicio" in df.columns:
                df = df.rename(columns={"exercicio": "ano"})
            df["ano"] = int(y)
            out.append(df)
    return pd.concat(out, ignore_index=True) if out else pd.DataFrame()

@st.cache_data(show_spinner=False)
def fs_receita_por_tipo(ano: int) -> pd.DataFrame:
    """
    Carrega receita_por_codigo_anual.csv e garante:
      - coluna 'tipo' (a partir de 'especificacao' não nula mais longa por código)
      - remove códigos vazios
    """
    df = fs_load_csv(ano, "receita_por_codigo_anual").copy()
    if df.empty:
        return df

    # normaliza
    for c in df.columns:
        if df[c].dtype == object:
            df[c] = df[c].astype(str).str.strip()
    if "codigo" not in df.columns:
        return pd.DataFrame()

    df = df[df["codigo"].astype(str).str.strip() != ""].copy()

    # cria 'tipo' = especificacao não nula mais longa por codigo
    if "especificacao" in df.columns:
        spec_fill = (
            df.loc[df["especificacao"].fillna("").astype(str).str.strip() != ""]
              .assign(_len=lambda d: d["especificacao"].astype(str).str.len())
              .sort_values(["codigo", "_len"], ascending=[True, False])
              .drop_duplicates("codigo")[["codigo", "especificacao"]]
              .rename(columns={"especificacao": "tipo"})
        )
        df = df.drop(columns=["especificacao"], errors="ignore").merge(spec_fill, on="codigo", how="left")
    else:
        df["tipo"] = ""

    # garante numéricos
    for c in ["previsto", "arrecadado"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0.0)

    return df

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
top_n = st.sidebar.slider("Top N", 5, 30, 15)
metrica_ent = st.sidebar.radio("Métrica para 'por Entidade'", ["pago", "liquidado", "empenhado"], index=0, horizontal=True)
busca_ent = st.sidebar.text_input("Filtro de entidade (contém)", value="").strip()

show_legend_codigo = st.sidebar.checkbox("Mostrar legenda por código no 'Receita por Tipo'", value=False)

st.title("📊 Monitor de Execução Orçamentária — Londrina")
info_source()
st.markdown("---")

# =========================
# Abas
# =========================
tab_resumo, tab_despesa, tab_receita, tab_serie = st.tabs(
    ["📌 Resumo", "🏛️ Despesa por Entidade", "💰 Receita por Tipo", "📈 Série Anual"]
)

# =========================
# Resumo (ano selecionado)
# =========================
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

# =========================
# Despesa por Entidade (ano)
# =========================
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

            # ordena por valor (original) e aplica escala só no y exibido
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
            fig = plot_bar_fmt(fig, "valor_escala", escala)
            st.plotly_chart(fig, use_container_width=True)

            with st.expander("Dados usados neste gráfico"):
                st.dataframe(ent_plot[["entidade", metrica_ent]].rename(columns={metrica_ent: "valor (bruto)"}))
                download_df_button(ent_plot[["entidade", metrica_ent]], f"despesa_por_entidade_{year}.csv", "Baixar CSV")
    else:
        st.info("Não há dados de despesa por entidade para o ano.")

# =========================
# Receita por Tipo (ano)
# =========================
with tab_receita:
    st.subheader("Receita por Tipo — ano selecionado")
    if USE_DB:
        rec_tipo = db_receita_por_tipo(year)
    else:
        rec_tipo = fs_receita_por_tipo(year)

    if not rec_tipo.empty:
        ycol = "arrecadado" if "arrecadado" in rec_tipo.columns else "previsto"
        rec_tipo = rec_tipo.copy()
        rec_tipo[ycol] = pd.to_numeric(rec_tipo[ycol], errors="coerce").fillna(0.0)

        # ordena e top N
        rec_tipo = rec_tipo.sort_values(ycol, ascending=False).head(top_n)

        # colunas auxiliares para visual
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
            hovertemplate="<b>%{x}</b>" +
                          ("<br>Código: %{customdata[1]}" if show_legend_codigo and "codigo" in rec_tipo.columns else "") +
                          "<br>" + f"{label_valor(escala)}: %{customdata[0]}<extra></extra>"
        )
        fig.update_xaxes(categoryorder="total descending")
        fig = plot_bar_fmt(fig, "valor_escala", escala)
        st.plotly_chart(fig, use_container_width=True)

        with st.expander("Dados usados neste gráfico"):
            cols = ["codigo", "tipo", ycol] if "codigo" in rec_tipo.columns else ["tipo", ycol]
            st.dataframe(rec_tipo[cols].rename(columns={ycol: "valor (bruto)"}))
            download_df_button(rec_tipo[cols], f"receita_por_tipo_{year}.csv", "Baixar CSV")
    else:
        st.info("Sem dados de receita por tipo para o ano.")

# =========================
# Evolução anual — série
# =========================
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
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("Sem série anual consolidada.")

st.markdown("---")
st.caption("No modo CSV, gere KPIs com `scripts/09_export_kpis.py` e faça commit em `data/kpis/`. No modo DB, os dados vêm direto do Neon.")
