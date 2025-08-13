#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
01_fetch_equiplano_ano.py  (CSV only)
Baixa CSVs do Equiplano (portal antigo, DisplayTag) para despesas:
  - empenhadas
  - liquidadas
  - pagas

Também oferece modo 'load' para carregar CSVs no Postgres (staging).

Exemplos:
  # Baixar 2018–2025 para raw/
  python scripts/01_fetch_equiplano_ano.py download --anos 2018-2025 --saida raw/ --verbose

  # Baixar apenas liquidadas e pagas de 2024–2025
  python scripts/01_fetch_equiplano_ano.py download --anos 2024-2025 --stages liquidadas,pagas --saida raw/

  # Carregar CSVs para staging
  export DATABASE_URL="postgresql://user:pass@host:5432/db"
  python scripts/01_fetch_equiplano_ano.py load --csv raw/ --table stg_execucao_orcamentaria --schema public --sep ";" --dedupe --add-year --year-col exercicio --verbose
"""
import argparse
import os
import re
import sys
import time
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple
from datetime import datetime, timezone
from urllib.parse import urljoin

import requests
import pandas as pd
from sqlalchemy import create_engine, text

try:
    from bs4 import BeautifulSoup  # pip install beautifulsoup4
except Exception:
    BeautifulSoup = None

# ===================== CONFIG DO PORTAL (CSV only) =====================
CONFIG = {
    "base_url": "http://portaltransparencia.londrina.pr.gov.br:8080",
    "stages": {
        "empenhadas": {
            "list_path": "/transparencia/despesaEmpenhada/listaAno",
            "params_map": {  # GET params esperados pelo portal
                "exercicio": "formulario.exercicio",
                "entidade": "formulario.codEntidade",
            },
            "entidade_all": "",   # string vazia = todas
        },
        "liquidadas": {
            "list_path": "/transparencia/despesaLiquidada/listaAno",
            "params_map": {
                "exercicio": "formulario.exercicio",
                "entidade": "formulario.codEntidade",
            },
            "entidade_all": "",
        },
        "pagas": {
            "list_path": "/transparencia/despesaPaga/listaDespesaPagaPorAno",
            "params_map": {
                "exercicio": "formulario.exercicio",
                "entidade": "formulario.codEntidade",
            },
            "entidade_all": "",
        },
    },
}

VALID_STAGES = list(CONFIG["stages"].keys())

# ===================== HTTP helpers =====================
DEFAULT_HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "pt-BR,pt;q=0.8,en-US;q=0.6,en;q=0.4",
}

def http_get(session: requests.Session, url: str, params=None, timeout=90, headers=None):
    hdrs = {**DEFAULT_HEADERS, **(headers or {})}
    r = session.get(url, params=params, timeout=timeout, headers=hdrs, allow_redirects=True)
    r.raise_for_status()
    return r

def http_post(session: requests.Session, url: str, data=None, timeout=90, headers=None):
    hdrs = {**DEFAULT_HEADERS, **(headers or {})}
    r = session.post(url, data=data, timeout=timeout, headers=hdrs, allow_redirects=True)
    r.raise_for_status()
    return r

def retry(fn, retries=3, backoff=1.5, verbose=False):
    last = None
    for i in range(1, retries + 1):
        try:
            return fn()
        except Exception as e:
            last = e
            if verbose:
                print(f"⚠️ tentativa {i}/{retries} falhou: {e}")
            if i < retries:
                time.sleep(backoff * i)
    raise last

def ensure_dir(p: Path):
    p.mkdir(parents=True, exist_ok=True)

# ===================== DisplayTag helpers =====================
# tolera "d-1234", "d-1234-" e "d-1234-qualquercoisa"
DISPLAYTAG_ID_RE = re.compile(r"\bd-(\d+)(?:-\w+)?")
CSV_ANCHOR_RE = re.compile(r'href="([^"]*?csv[^"]*)"', re.IGNORECASE)

def looks_like_html(content: bytes) -> bool:
    head = content[:2048].lstrip().lower()
    return head.startswith(b"<!doctype html") or head.startswith(b"<html")

def content_is_csv(resp: requests.Response, content: bytes) -> bool:
    ct = (resp.headers.get("Content-Type") or "").lower()
    if ("text/csv" in ct) or ("application/csv" in ct) or ("octet-stream" in ct and not looks_like_html(content)):
        return True
    if looks_like_html(content):
        return False
    # heurística leve caso o servidor não mande Content-Type correto
    try:
        sample = content[:4096].decode("utf-8")
    except UnicodeDecodeError:
        sample = content[:4096].decode("latin-1", errors="ignore")
    return (";" in sample or "," in sample) and ("\n" in sample or "\r" in sample)

def extract_displaytag_id(html: str) -> Optional[str]:
    m = DISPLAYTAG_ID_RE.search(html)
    return m.group(1) if m else None

def extract_csv_anchor(html: str, base_ref: str) -> Optional[str]:
    m = CSV_ANCHOR_RE.search(html)
    if m:
        return urljoin(base_ref, m.group(1))
    if BeautifulSoup:
        soup = BeautifulSoup(html, "html.parser")
        for a in soup.find_all("a", href=True):
            if "csv" in a["href"].lower():
                return urljoin(base_ref, a["href"])
    return None

# --- helper para salvar CSV sempre em UTF-8 ---
def write_csv_normalized(dest: Path, content: bytes, resp: Optional[requests.Response] = None, verbose: bool = False):
    """
    Salva 'content' como UTF-8. Se o header indicar ISO-8859-1 (latin-1) ou
    se a decodificação UTF-8 falhar, decodifica como latin-1 e regrava em UTF-8.
    """
    encoding_hint = None
    if resp:
        ct = (resp.headers.get("Content-Type") or "").lower()
        if "iso-8859-1" in ct or "latin-1" in ct:
            encoding_hint = "latin-1"

    if encoding_hint:
        txt = content.decode("latin-1", errors="ignore")
    else:
        try:
            txt = content.decode("utf-8")
        except UnicodeDecodeError:
            txt = content.decode("latin-1", errors="ignore")

    dest.write_text(txt, encoding="utf-8")
    if verbose:
        print(f"📝 Arquivo salvo como UTF-8: {dest}")

# ===================== Baixa CSV para 1 stage/ano =====================
def build_params(stage_cfg: Dict, ano: int) -> Dict:
    pm = stage_cfg["params_map"]
    return {
        pm["exercicio"]: str(ano),
        pm["entidade"]: stage_cfg.get("entidade_all", ""),
    }

def try_export_get(session: requests.Session, list_url: str, base_params: Dict, d_id: str,
                   timeout: int, retries: int, backoff: float, verbose: bool) -> Optional[bytes]:
    """
    Export típica do portal:
      - d-<id>-e=1   (export flag)
      - 6578706f7274=1  (param "export" em ASCII-hex)
    Testamos algumas variações compatíveis com DisplayTag.
    """
    variants = []
    def add(extra):
        p = dict(base_params); p.update(extra); variants.append(p)

    # comprovado no DevTools:
    add({f"d-{d_id}-e": "1", "6578706f7274": "1"})
    # outras variações ocasionais:
    add({f"d-{d_id}-o": "csv", "6578706f7274": "1"})
    add({f"d-{d_id}-e": "1", "export": "1"})
    add({f"d-{d_id}-o": "csv", "exportType": "csv"})
    add({"displaytag_export": "true", f"d-{d_id}-e": "1"})

    for i, params in enumerate(variants, 1):
        if verbose:
            extra = {k:v for k,v in params.items() if k not in base_params}
            print(f"🔁 GET export {i}/{len(variants)} extras={extra}")
        resp = retry(lambda: http_get(session, list_url, params=params, timeout=timeout,
                                      headers={"Referer": list_url}), retries, backoff, verbose)
        if content_is_csv(resp, resp.content):
            return resp.content
    return None

def try_export_post(session: requests.Session, list_url: str, base_params: Dict, d_id: Optional[str], html: str,
                    timeout: int, retries: int, backoff: float, verbose: bool) -> Optional[bytes]:
    """
    Alguns módulos usam submit do <form> via POST para exportar.
    """
    if not BeautifulSoup:
        return None
    soup = BeautifulSoup(html, "html.parser")
    form = soup.find("form")
    if not form:
        return None
    action = urljoin(list_url, form.get("action") or "")
    payload: Dict[str, str] = {}
    for inp in form.find_all(["input", "select", "textarea"]):
        name = inp.get("name")
        if not name:
            continue
        val = inp.get("value", "")
        if inp.name == "select":
            opt = inp.find("option", selected=True) or inp.find("option")
            val = opt.get("value", "") if opt else ""
        payload[name] = val

    # garante filtros e flags de export
    payload.update(base_params)
    payload["6578706f7274"] = "1"
    if d_id:
        payload[f"d-{d_id}-e"] = "1"

    if verbose:
        print(f"📝 POST export form → {action}")
    resp = retry(lambda: http_post(session, action, data=payload, timeout=timeout,
                                   headers={"Referer": list_url, "Content-Type": "application/x-www-form-urlencoded"}),
                 retries, backoff, verbose)
    if content_is_csv(resp, resp.content):
        return resp.content
    return None

def fetch_one_csv(session: requests.Session, base_url: str, stage: str, ano: int, out_dir: Path,
                  timeout=90, retries=3, backoff=1.5, verbose=False) -> Path:
    stage_cfg = CONFIG["stages"][stage]
    list_url = urljoin(base_url, stage_cfg["list_path"])
    params = build_params(stage_cfg, ano)

    if verbose:
        print(f"📄 GET lista: {list_url} params={params}")

    # 1) abre a lista para capturar cookies e d-id
    resp = retry(lambda: http_get(session, list_url, params=params, timeout=timeout,
                                  headers={"Referer": list_url}), retries, backoff, verbose)
    html = resp.text

    # 2) se houver link <a ... csv>, tenta direto
    csv_link = extract_csv_anchor(html, list_url)
    if csv_link:
        if verbose:
            print(f"⬇️ link CSV encontrado: {csv_link}")
        csv_resp = retry(lambda: http_get(session, csv_link, timeout=timeout,
                                          headers={"Referer": list_url}), retries, backoff, verbose)
        if content_is_csv(csv_resp, csv_resp.content):
            folder = out_dir / stage; ensure_dir(folder)
            dest = folder / f"equiplano_{stage}_ano{ano}.csv"
            write_csv_normalized(dest, csv_resp.content, resp=csv_resp, verbose=verbose)
            if verbose: print(f"✅ Salvo: {dest}")
            return dest
        if verbose:
            print("ℹ️ Link 'csv' não retornou CSV. Tentando flags de export…")

    # 3) flags de export (GET) com d-id
    d_id = extract_displaytag_id(html)
    if d_id:
        content = try_export_get(session, list_url, params, d_id, timeout, retries, backoff, verbose)
        if content:
            folder = out_dir / stage; ensure_dir(folder)
            dest = folder / f"equiplano_{stage}_ano{ano}.csv"
            write_csv_normalized(dest, content, resp=None, verbose=verbose)
            if verbose: print(f"✅ Salvo: {dest}")
            return dest

    # 3.1) fallback: tentar export SEM id (algumas telas aceitam)
    if not d_id:
        if verbose:
            print("ℹ️ Não achei d-id; tentando export sem id…")
        for extras in (
            {"6578706f7274": "1"},
            {"exportType": "csv"},
            {"displaytag_export": "true"},
            {"export": "csv"},
        ):
            test = dict(params); test.update(extras)
            resp_try = retry(lambda: http_get(session, list_url, params=test, timeout=timeout,
                                              headers={"Referer": list_url}), retries, backoff, verbose)
            if content_is_csv(resp_try, resp_try.content):
                folder = out_dir / stage; ensure_dir(folder)
                dest = folder / f"equiplano_{stage}_ano{ano}.csv"
                write_csv_normalized(dest, resp_try.content, resp=resp_try, verbose=verbose)
                if verbose: print(f"✅ Salvo (sem id): {dest}")
                return dest

    # 4) fallback via POST do form (alguns módulos)
    content = try_export_post(session, list_url, params, d_id, html, timeout, retries, backoff, verbose)
    if content:
        folder = out_dir / stage; ensure_dir(folder)
        dest = folder / f"equiplano_{stage}_ano{ano}.csv"
        write_csv_normalized(dest, content, resp=None, verbose=verbose)
        if verbose: print(f"✅ Salvo: {dest}")
        return dest

    # 5) erro + dump de HTML
    debug_dir = out_dir / "_html_debug"; ensure_dir(debug_dir)
    dbg = debug_dir / f"{stage}_{ano}_export_falhou.html"
    dbg.write_text(html, encoding="utf-8")
    raise RuntimeError(f"Export CSV falhou para {stage}/{ano}. HTML salvo: {dbg}")

# ===================== Utilidades de carga (Postgres) =====================
def md5_row(values):
    s = "||".join("" if v is None else str(v) for v in values)
    import hashlib
    return hashlib.md5(s.encode("utf-8")).hexdigest()

def safe_idx_name(base: str) -> str:
    name = re.sub(r"[^a-z0-9_]+", "_", base.lower())
    if len(name) > 60:
        name = name[:60]
    if not name:
        name = "idx_hash"
    return name

def ensure_table_and_columns(engine, schema: str, table: str, df_cols):
    cols_sql = ", ".join([f'"{c}" TEXT' for c in df_cols])
    sql_create = f'''
    CREATE TABLE IF NOT EXISTS "{schema}"."{table}" (
        {cols_sql},
        "id_linha_hash" TEXT,
        "dt_extracao"   TIMESTAMPTZ
    );'''
    sql_add_cols = [f'ALTER TABLE "{schema}"."{table}" ADD COLUMN IF NOT EXISTS "{c}" TEXT;' for c in df_cols]
    sql_add_cols += [
        f'ALTER TABLE "{schema}"."{table}" ADD COLUMN IF NOT EXISTS "id_linha_hash" TEXT;',
        f'ALTER TABLE "{schema}"."{table}" ADD COLUMN IF NOT EXISTS "dt_extracao" TIMESTAMPTZ;'
    ]
    with engine.begin() as con:
        con.execute(text(sql_create))
        for stmt in sql_add_cols:
            con.execute(text(stmt))

def add_hash_index(engine, schema: str, table: str):
    idx = safe_idx_name(f"{table}_hash_idx")
    # corrigir caso apareça chave duplicada no nome
    idx = idx.replace("}}", "}")
    sql = f'''
    DO $$
    BEGIN
      IF NOT EXISTS (
        SELECT 1 FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE c.relname = '{idx}' AND n.nspname = '{schema}'
      ) THEN
        EXECUTE 'CREATE INDEX {idx} ON "{schema}"."{table}" ("id_linha_hash")';
      END IF;
    END$$;'''
    with engine.begin() as con:
        con.execute(text(sql))

def insert_via_temp(con, schema, table, df, dedupe: bool):
    temp_table = f"tmp_{table}_load"
    cols_sql = ", ".join([f'"{c}" TEXT' for c in df.columns])
    con.execute(text(f'DROP TABLE IF EXISTS "{temp_table}";'))
    con.execute(text(f'CREATE TEMP TABLE "{temp_table}" ({cols_sql}) ON COMMIT DROP;'))

    chunksize = 5000
    for i in range(0, len(df), chunksize):
        df.iloc[i:i+chunksize].to_sql(temp_table, con, if_exists="append", index=False, method="multi")

    dest_cols = ", ".join([f'"{c}"' for c in df.columns])
    select_cols = ", ".join([f't."{c}"' for c in df.columns])

    if dedupe:
        sql = f'''
            INSERT INTO "{schema}"."{table}" ({dest_cols})
            SELECT {select_cols}
            FROM "{temp_table}" t
            LEFT JOIN "{schema}"."{table}" d ON t."id_linha_hash" = d."id_linha_hash"
            WHERE d."id_linha_hash" IS NULL;
        '''
    else:
        sql = f'''
            INSERT INTO "{schema}"."{table}" ({dest_cols})
            SELECT {select_cols}
            FROM "{temp_table}" t;
        '''
    con.execute(text(sql))

def detect_exercicio_col(df: pd.DataFrame) -> Optional[str]:
    for col in df.columns:
        c = col.strip().lower().replace("ç", "c").replace("í", "i")
        if "exercicio" in c or c == "ano":
            return col
    return None

def clean_and_standardize(df: pd.DataFrame, add_year_flag: bool, year_col_name: str, verbose: bool) -> pd.DataFrame:
    col_ex = detect_exercicio_col(df)
    if col_ex is None:
        if add_year_flag:
            col_ex = year_col_name
            if col_ex not in df.columns:
                df[col_ex] = ""
        else:
            raise ValueError(f"Nenhuma coluna 'Exercício' ou 'ano'. Colunas: {list(df.columns)}")

    # remove linhas 'total' nessa coluna
    before = len(df)
    df = df[~df[col_ex].astype(str).str.contains("total", case=False, na=False)].copy()
    removed = before - len(df)
    if verbose and removed:
        print(f"🧹 removidas linhas 'total': {removed}")

    df["ano"] = df[col_ex].astype(str).str.extract(r"(\d{4})")
    df["ano"] = pd.to_numeric(df["ano"], errors="coerce")
    if not add_year_flag:
        df = df.dropna(subset=["ano"]).copy()
    try:
        df["ano"] = df["ano"].astype("Int64")
    except Exception:
        pass
    return df

def extract_year_from_filename(path: Path) -> Optional[int]:
    name = path.name
    m = re.search(r"(\d{4})", name)
    return int(m.group(1)) if m else None

def expand_inputs(paths: List[str], exclude_dirs: Set[str], verbose: bool) -> List[Path]:
    csvs: List[Path] = []
    exclude_dirs_lower = {e.lower() for e in exclude_dirs}
    def should_exclude(p: Path) -> bool:
        return bool({x.lower() for x in p.parts}.intersection(exclude_dirs_lower))

    for p in paths:
        pth = Path(p)
        if pth.exists():
            if pth.is_file() and pth.suffix.lower() == ".csv" and not should_exclude(pth):
                csvs.append(pth)
            elif pth.is_dir():
                for f in pth.rglob("*.csv"):
                    if f.is_file() and not should_exclude(f):
                        csvs.append(f)
            continue
        for f in Path().glob(p):
            if f.is_file() and f.suffix.lower() == ".csv" and not should_exclude(f):
                csvs.append(f)
            if f.is_dir():
                for g in f.rglob("*.csv"):
                    if g.is_file() and not should_exclude(g):
                        csvs.append(g)
    csvs = sorted(set(csvs))
    if verbose:
        print(f"🔎 encontrados {len(csvs)} CSV(s).")
        for s in csvs[:10]:
            print("  •", s)
    return csvs

def load_one(engine, schema, table, csv_path, sep, dedupe, add_year, year_col, verbose):
    csv_path = Path(csv_path)
    if verbose:
        print(f"\n⚙️  Carregando {csv_path} → {schema}.{table}")

    # os arquivos baixados agora já estão em UTF-8
    df = pd.read_csv(csv_path, sep=sep, dtype=str, keep_default_na=False, na_filter=False, encoding="utf-8-sig")
    df = clean_and_standardize(df, add_year_flag=add_year, year_col_name=year_col, verbose=verbose)

    if add_year and (df["ano"].isna().any() or (str(df["ano"].dtype).startswith("Int") and df["ano"].isna().any())):
        inferred = extract_year_from_filename(csv_path)
        if inferred:
            df["ano"] = df["ano"].fillna(inferred)

    # marca 'tipo' pelo caminho
    tipo = "desconhecido"
    n = csv_path.name.lower()
    if "empenhad" in n:
        tipo = "despesas_empenhadas"
    elif "liquidad" in n:
        tipo = "despesas_liquidadas"
    elif "paga" in n:
        tipo = "despesas_pagas"
    df["tipo"] = tipo

    # limpa colunas internas se houver
    for col in ["id_linha_hash", "dt_extracao"]:
        if col in df.columns:
            df.drop(columns=[col], inplace=True)

    ensure_table_and_columns(engine, schema, table, list(df.columns))
    df["id_linha_hash"] = df.apply(lambda r: md5_row([r.get(c) for c in df.columns if c not in ("id_linha_hash","dt_extracao")]), axis=1)
    df["dt_extracao"] = datetime.now(timezone.utc).isoformat()
    add_hash_index(engine, schema, table)

    with engine.begin() as con:
        insert_via_temp(con, schema, table, df, dedupe)

    if verbose:
        print("✅ Inserido.")

# ===================== CLI =====================
def parse_anos(s: str) -> List[int]:
    s = s.strip()
    if "-" in s:
        a, b = s.split("-", 1)
        return list(range(int(a), int(b) + 1))
    parts = re.split(r"[,\s]+", s)
    return [int(x) for x in parts if x]

def main():
    ap = argparse.ArgumentParser(description="Baixa CSVs do Equiplano (DisplayTag) e/ou carrega no Postgres.")
    sub = ap.add_subparsers(dest="mode", required=True)

    # download
    dl = sub.add_parser("download", help="Baixa CSVs por anos e stages (empenhadas, liquidadas, pagas).")
    dl.add_argument("--anos", required=True, help="Ex.: 2018-2025 ou 2018,2019,2020")
    dl.add_argument("--stages", default=",".join(VALID_STAGES), help=f"Default: {','.join(VALID_STAGES)}")
    dl.add_argument("--saida", default="raw", help="Diretório de saída (default: raw)")
    dl.add_argument("--timeout", type=int, default=90)
    dl.add_argument("--retries", type=int, default=3)
    dl.add_argument("--backoff", type=float, default=1.5)
    dl.add_argument("--verbose", action="store_true")

    # load
    ld = sub.add_parser("load", help="Carrega CSV(s) para Postgres.")
    ld.add_argument("--csv", required=True, nargs="+", help="Caminhos/pastas/globs (ex.: raw/ ou raw/*.csv)")
    ld.add_argument("--table", required=True, help='Tabela destino (ex.: "stg_execucao_orcamentaria")')
    ld.add_argument("--schema", default="public")
    ld.add_argument("--sep", default=";", help="Separador dos CSVs (default ;)")
    ld.add_argument("--dedupe", action="store_true")
    ld.add_argument("--add-year", action="store_true")
    ld.add_argument("--year-col", default="ano")
    ld.add_argument("--exclude-dir", nargs="*", default=[], help="Pastas a ignorar")
    ld.add_argument("--verbose", action="store_true")

    args = ap.parse_args()

    if args.mode == "download":
        base_url = CONFIG["base_url"]
        stages = [s.strip() for s in args.stages.split(",") if s.strip()]
        for s in stages:
            if s not in VALID_STAGES:
                print(f"⚠️ Stage inválido: {s}. Válidos: {', '.join(VALID_STAGES)}", file=sys.stderr)
                sys.exit(2)

        anos = parse_anos(args.anos)
        out_dir = Path(args.saida)
        ensure_dir(out_dir)

        with requests.Session() as sess:
            for ano in anos:
                for stage in stages:
                    try:
                        fetch_one_csv(
                            sess, base_url, stage, ano, out_dir,
                            timeout=args.timeout, retries=args.retries, backoff=args.backoff, verbose=args.verbose
                        )
                    except Exception as e:
                        print(f"❌ Falha em {stage}/{ano}: {e}", file=sys.stderr)
                        sys.exit(2)

        print("✅ Download concluído.")
        return

    # load
    url = os.environ.get("DATABASE_URL")
    if not url:
        print("❌ DATABASE_URL não definido.", file=sys.stderr)
        sys.exit(1)

    engine = create_engine(url)
    files: List[Path] = []
    exclude_dirs = set(getattr(args, "exclude_dir", []))
    for pattern in args.csv:
        files.extend(expand_inputs([pattern], exclude_dirs, args.verbose))
    files = sorted(set(files))
    if not files:
        print("⚠️  Nenhum CSV encontrado.", file=sys.stderr)
        sys.exit(3)

    for f in files:
        try:
            load_one(engine, args.schema, args.table, str(f), args.sep, args.dedupe, args.add_year, args.year_col, args.verbose)
        except Exception as e:
            print(f"❌ Erro ao carregar {f}: {e}", file=sys.stderr)
            sys.exit(2)

if __name__ == "__main__":
    main()