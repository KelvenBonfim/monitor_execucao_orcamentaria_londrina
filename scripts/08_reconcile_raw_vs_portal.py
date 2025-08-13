#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
08_reconcile_raw_vs_portal.py — Reconciliador "estrito" entre RAW e snapshot novo do portal
para os três primeiros scripts do projeto:

  • 01_fetch_equiplano_ano.py   → Despesas (empenhadas, liquidadas, pagas) [CSV DisplayTag]
  • 02_fetch_receita_prev_arrec.py + 03_anexo10_pdf_to_csv.py → Receita Anexo 10 (PDF→CSV)

O script:
  1) Lê os arquivos já existentes no RAW.
  2) Baixa os mesmos dados do portal AGORA (usando a MESMA rotina do 01 para DisplayTag e
     o MESMO parser do 03 para o PDF do Anexo 10).
  3) Normaliza, remove linhas "TOTAL", autodetecta colunas de valores e soma.
  4) Compara RAW x PORTAL e gera relatórios de difs.

Saídas (em --outdir, default: outputs/reconcile_raw_vs_portal):
  - D_despesas_reconcile.csv
  - D_columns_used.csv
  - R_receita_reconcile.csv
  - SUMMARY.csv
  - raw_snapshots/…    (os snapshots novos baixados do portal)

Uso típico:
  python scripts/08_reconcile_raw_vs_portal.py \
    --anos 2018-2025 \
    --rawdir raw \
    --outdir outputs/reconcile_raw_vs_portal \
    --stages empenhadas,liquidadas,pagas \
    --include-receita \
    --timeout 90 --retries 3 --backoff 1.5 \
    --verbose
"""

import argparse
import glob
import os
import re
import sys
import time
import unicodedata
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from urllib.parse import urljoin

import pandas as pd
import requests

# ===================== CONFIG base (igual ao 01) =====================

CONFIG = {
    "base_url": "http://portaltransparencia.londrina.pr.gov.br:8080",
    "stages": {
        "empenhadas": {
            "list_path": "/transparencia/despesaEmpenhada/listaAno",
            "params_map": {"exercicio": "formulario.exercicio", "entidade": "formulario.codEntidade"},
            "entidade_all": "",
        },
        "liquidadas": {
            "list_path": "/transparencia/despesaLiquidada/listaAno",
            "params_map": {"exercicio": "formulario.exercicio", "entidade": "formulario.codEntidade"},
            "entidade_all": "",
        },
        "pagas": {
            # igual ao 01
            "list_path": "/transparencia/despesaPaga/listaDespesaPagaPorAno",
            "params_map": {"exercicio": "formulario.exercicio", "entidade": "formulario.codEntidade"},
            "entidade_all": "",
        },
    },
}
VALID_STAGES = list(CONFIG["stages"].keys())

# ===================== Helpers HTTP / DisplayTag (copiados do 01) =====================

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
    try:
        from bs4 import BeautifulSoup  # optional
        soup = BeautifulSoup(html, "html.parser")
        for a in soup.find_all("a", href=True):
            if "csv" in a["href"].lower():
                return urljoin(base_ref, a["href"])
    except Exception:
        pass
    return None

def write_csv_text(dest: Path, content: bytes, resp: Optional[requests.Response] = None):
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

def build_params(stage_cfg: Dict, ano: int) -> Dict:
    pm = stage_cfg["params_map"]
    return {pm["exercicio"]: str(ano), pm["entidade"]: stage_cfg.get("entidade_all", "")}

def try_export_get(session: requests.Session, list_url: str, base_params: Dict, d_id: str,
                   timeout: int, retries: int, backoff: float, verbose: bool) -> Optional[bytes]:
    variants = []
    def add(extra):
        p = dict(base_params); p.update(extra); variants.append(p)
    add({f"d-{d_id}-e": "1", "6578706f7274": "1"})
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
    try:
        from bs4 import BeautifulSoup
    except Exception:
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

def fetch_portal_csv(session: requests.Session, base_url: str, stage: str, ano: int, out_dir: Path,
                     timeout=90, retries=3, backoff=1.5, verbose=False) -> Path:
    cfg = CONFIG["stages"][stage]
    list_url = urljoin(base_url, cfg["list_path"])
    params = build_params(cfg, ano)
    if verbose:
        print(f"📄 GET lista: {list_url} params={params}")
    resp = retry(lambda: http_get(session, list_url, params=params, timeout=timeout,
                                  headers={"Referer": list_url}), retries, backoff, verbose)
    html = resp.text

    # link csv direto?
    csv_link = extract_csv_anchor(html, list_url)
    folder = out_dir / "raw_snapshots" / f"{ano}"
    folder.mkdir(parents=True, exist_ok=True)
    dest = folder / f"equiplano_{stage}_ano{ano}.csv"

    if csv_link:
        if verbose:
            print(f"⬇️ link CSV encontrado: {csv_link}")
        csv_resp = retry(lambda: http_get(session, csv_link, timeout=timeout,
                                          headers={"Referer": list_url}), retries, backoff, verbose)
        if content_is_csv(csv_resp, csv_resp.content):
            write_csv_text(dest, csv_resp.content, resp=csv_resp)
            return dest

    # export GET com d-id
    d_id = extract_displaytag_id(html)
    if d_id:
        content = try_export_get(session, list_url, params, d_id, timeout, retries, backoff, verbose)
        if content:
            write_csv_text(dest, content, resp=None)
            return dest

    # export sem id (fallback)
    if not d_id:
        for extras in ({"6578706f7274": "1"}, {"exportType": "csv"}, {"displaytag_export": "true"}, {"export": "csv"}):
            test = dict(params); test.update(extras)
            resp_try = retry(lambda: http_get(session, list_url, params=test, timeout=timeout,
                                              headers={"Referer": list_url}), retries, backoff, verbose)
            if content_is_csv(resp_try, resp_try.content):
                write_csv_text(dest, resp_try.content, resp=resp_try)
                return dest

    # POST fallback
    content = try_export_post(session, list_url, params, d_id, html, timeout, retries, backoff, verbose)
    if content:
        write_csv_text(dest, content, resp=None)
        return dest

    dbg = (out_dir / "_html_debug")
    dbg.mkdir(parents=True, exist_ok=True)
    dump = dbg / f"{stage}_{ano}_export_falhou.html"
    dump.write_text(html, encoding="utf-8")
    raise RuntimeError(f"Export CSV falhou para {stage}/{ano}. HTML salvo: {dump}")

# ===================== Normalização / soma de DESPESAS =====================

def norm_key(s: str) -> str:
    if s is None:
        return ""
    s = "".join(ch for ch in unicodedata.normalize("NFD", s) if unicodedata.category(ch) != "Mn")
    s = s.lower()
    s = re.sub(r"[^a-z0-9]+", "_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    return s

def to_numeric_br(x: str) -> float:
    if x is None:
        return 0.0
    s = str(x).strip().replace("\xa0", " ")  # NBSP
    neg = s.startswith("(") and s.endswith(")")
    s = s.replace("(", "").replace(")", "")
    if "," in s and s.count(",") == 1:
        s = s.replace(".", "").replace(",", ".")
    s = re.sub(r"[^0-9.\-]", "", s)
    if s in ("", "-", ".", "-.", ".-"):
        v = 0.0
    else:
        try:
            v = float(s)
        except Exception:
            v = 0.0
    return -v if neg else v

def read_csv_any(path: Path) -> pd.DataFrame:
    try:
        return pd.read_csv(path, sep=";", dtype=str, encoding="utf-8-sig")
    except Exception:
        return pd.read_csv(path, sep=",", dtype=str, encoding="utf-8-sig")

def drop_total_rows(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df.copy()
    mask = pd.Series(False, index=df.index)
    # considere apenas colunas textuais
    text_cols = [c for c in df.columns if df[c].dtype == object]
    if not text_cols:
        return df.copy()
    # remove somente linhas onde ALGUMA coluna textual seja exatamente "TOTAL"
    rx = re.compile(r"^\s*total\s*$", re.IGNORECASE)
    for c in text_cols:
        mask = mask | df[c].astype(str).str.match(rx, na=False)
    return df.loc[~mask].copy()

def find_cols(df: pd.DataFrame, must: List[str], any_of: Optional[List[str]] = None) -> List[str]:
    nk = {col: norm_key(col) for col in df.columns}
    # ampliar sinônimos leves
    syn = {
        "empenhad": ["empenhad", "empenho"],
        "liquid":   ["liquid", "liquidad", "liquidac"],
        "pago":     ["pago", "pagamento"],
        "orc":      ["orc", "orcamento"],
        "restos":   ["restos", "rap", "a_pagar", "apagar", "pagar"],
    }
    def expand(tokens: List[str]) -> List[str]:
        out = []
        for t in tokens:
            out.extend(syn.get(t, [t]))
        return out

    must_expanded   = expand(must)
    any_of_expanded = expand(any_of or [])

    out = []
    for col, key in nk.items():
        ok_must = all(any(tok in key for tok in must_expanded) for _ in [0]) \
                  if must_expanded else True
        ok_any  = any(tok in key for tok in any_of_expanded) if any_of_expanded else True
        if ok_must and ok_any:
            out.append(col)
    return out

def autodetect_sum_despesa(df: pd.DataFrame, stage: str) -> Tuple[float, List[str], int]:
    df2 = drop_total_rows(df)
    removed = len(df) - len(df2)
    used: List[str] = []
    total = 0.0

    if stage == "empenhadas":
        used = find_cols(df2, must=["empenhad"])
    elif stage == "liquidadas":
        cols_orc = find_cols(df2, must=["liquid"], any_of=["orc", "orcamento"])
        cols_rap = find_cols(df2, must=["liquid"], any_of=["restos"])
        used = cols_orc + [c for c in cols_rap if c not in cols_orc]
        if not used:
            used = find_cols(df2, must=["liquid"])
    elif stage == "pagas":
        cols_orc = find_cols(df2, must=["pago"], any_of=["orc", "orcamento"])
        cols_rap = find_cols(df2, must=["pago"], any_of=["restos"])
        used = cols_orc + [c for c in cols_rap if c not in cols_orc]
        if not used:
            used = find_cols(df2, must=["pago"])

    if not used:
        # hard fail para te avisar que o layout/nomes mudaram
        raise ValueError(f"Nenhuma coluna de valor detectada para stage={stage}. Colunas: {list(df.columns)}")

    for c in used:
        total += df2[c].map(to_numeric_br).sum()

    return float(total), used, removed

# ===================== Receita (Anexo 10) — downloader + parser (igual ao 02/03) =====================

BASE = CONFIG["base_url"]
URL_PROCESS = f"{BASE}/transparencia/execucaoOrcamentariaAnexo10ComparativoDaReceitaPrevistaComArrecadada/process"

ENTIDADES_DEFAULT: List[Tuple[int, str, str]] = [
    (483, "Administração dos Cemitérios e Serviços Funerários de Londrina - ACESF", "AUTARQUIA"),
    (482, "Autarquia Municipal de Saúde - AMS", "AUTARQUIA"),
    (486, "Caixa de Assist.Aposent. Pensões dos Servidores Municipais de Londrina", "AUTARQUIA"),
    (481, "Câmara Municipal de Londrina", "CAMARA"),
    (488, "Fundação de Esportes de Londrina", "AUTARQUIA"),
    (484, "Fundo de Assistência à Saúde dos Servidores Municipais de Londrina ", "AUTARQUIA"),
    (485, "Fundo de Previdência Social dos Servidores Municipais de Londrina ", "FUNDO_PREVIDENCIA"),
    (487, "Fundo de Urbanização de Londrina", "NAO_ENUMERADO"),
    (406, "Fundo Municipal de Saúde de Londrina", "AUTARQUIA"),
    (490, "Instituto de Desenvolvimento de Londrina - CODEL", "AUTARQUIA"),
    (489, "Instituto de Pesquisa e Planejamento Urbano de Londrina - IPPUL", "AUTARQUIA"),
    (480, "Prefeitura do Município de Londrina", "PREFEITURA"),
]

def parse_anos(s: str) -> List[int]:
    s = (s or "").strip()
    if "-" in s:
        a, b = s.split("-", 1)
        return list(range(int(a), int(b) + 1))
    return [int(x) for x in re.split(r"[,\s]+", s) if x.strip()]

def parse_entities_arg(arg: Optional[str]) -> List[Tuple[int, str, str]]:
    if not arg:
        return ENTIDADES_DEFAULT
    wanted = {int(x.strip()) for x in arg.split(",") if x.strip()}
    lookup = {cod: (cod, nome, tipo) for cod, nome, tipo in ENTIDADES_DEFAULT}
    out: List[Tuple[int, str, str]] = []
    for cod in sorted(wanted):
        out.append(lookup.get(cod, (cod, f"Entidade {cod}", "AUTARQUIA")))
    return out

def anexo10_payload(year: int, entidades: List[Tuple[int, str, str]]) -> Dict[str, str]:
    payload: Dict[str, str] = {
        "formulario.exercicio": str(year),
        "formulario.mesFinal": "12",
        "formulario.previsaoAnexo10Receitas": "1",
        "formulario.nrPaginaInicial": "1",
        "formulario.imprimirApenasResumo": "true",
        "formulario.detalharPorFonteRecurso": "true",
        "formulario.incluirContasSemMovimento": "true",
        "formulario.tpFormatoExterno": "PDF",
    }
    for i, (cod, nome, tipo) in enumerate(entidades):
        payload[f"formulario.seletorEntidades.itens[{i}].objeto.codEntidade"] = str(cod)
        payload[f"formulario.seletorEntidades.itens[{i}].objeto.nome"] = nome
        payload[f"formulario.seletorEntidades.itens[{i}].objeto.tipoEntidade"] = tipo
        payload[f"formulario.seletorEntidades.itens[{i}].selecionado"] = "true"
    return payload

def download_anexo10_pdf(year: int, out_dir: Path, timeout: int, retries: int, backoff: float, entidades_arg: Optional[str], verbose: bool) -> Path:
    entidades = parse_entities_arg(entidades_arg)
    payload = anexo10_payload(year, entidades)

    s = requests.Session()
    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "Origin": BASE,
        "Referer": f"{BASE}/transparencia/execucaoOrcamentariaAnexo10ComparativoDaReceitaPrevistaComArrecadada",
        "User-Agent": DEFAULT_HEADERS["User-Agent"],
        "Accept": "application/pdf,application/octet-stream,*/*;q=0.8",
        "Accept-Language": DEFAULT_HEADERS["Accept-Language"],
        "Connection": "close",
    }

    out_dir.mkdir(parents=True, exist_ok=True)
    out_pdf = out_dir / f"{year}-12-31_anexo10_prev_arrec.pdf"
    last_err: Optional[Exception] = None
    for attempt in range(1, retries + 1):
        try:
            if verbose:
                print(f"→ Anexo10 {year}: tentativa {attempt}/{retries}…")
            r = s.post(URL_PROCESS, data=payload, headers=headers, timeout=timeout)
            content = r.content or b""
            if content[:4] != b"%PDF":
                raise RuntimeError("Conteúdo não parece PDF (ou sessão/params inválidos).")
            out_pdf.write_bytes(content)
            if verbose:
                print(f"✅ PDF salvo: {out_pdf} ({len(content)} bytes)")
            return out_pdf
        except Exception as e:
            last_err = e
            time.sleep(max(1.5, backoff * attempt))
    raise RuntimeError(f"Falha ao baixar Anexo 10 {year}: {last_err}")

# Parser do 03 (resumido para extrair totais de Previsão/Arrecadação)
import pdfplumber

NUM_BR = r"-?\(?\s*(?:\d{1,3}(?:\.\d{3})*|\d+),\d{2}\s*\)?"
ROW_TAIL_RE = re.compile(rf"(.*?)\s+({NUM_BR})\s+({NUM_BR})\s+({NUM_BR})\s+({NUM_BR})\s*$")
COD_ROW_RE = re.compile(r"^\s*(TOTAL|\d{1,2})\b\s*(.*)$", re.IGNORECASE)

def is_columns_header(line: str) -> bool:
    s = line.casefold()
    return "código" in s and "especificação" in s

def looks_like_header(line: str) -> bool:
    s = line.casefold()
    return any(key in s for key in ("consolidação geral","consolidacao geral","anexo 10","página","pagina","conjunto de informações","entidades consolidadas"))

def normalize_number_br_to_float(txt: str) -> Optional[float]:
    if txt is None:
        return None
    s = str(txt).strip()
    if s.startswith("(") and s.endswith(")"):
        s = s[1:-1]
    s = s.replace(".", "").replace("\xa0", "").replace(" ", "")
    s = s.replace(",", ".")
    s = re.sub(r"[^0-9.\-]", "", s)
    if s in ("", "-", ".", "-.", ".-"):
        return None
    try:
        return float(s)
    except Exception:
        return None

def parse_table_lines(lines: List[str]) -> pd.DataFrame:
    start_idx = None
    for i, ln in enumerate(lines):
        if is_columns_header(ln):
            start_idx = i + 1
            break
    if start_idx is None:
        return pd.DataFrame(columns=["codigo","especificacao","subitem","previsao","arrecadacao","para_mais","para_menos"])
    rows = []
    current_code = None
    buffer = ""
    for raw in lines[start_idx:]:
        line = raw.strip()
        if not line:
            continue
        if looks_like_header(line):
            break
        text_for_match = (buffer + " " + line).strip() if buffer else line
        m = ROW_TAIL_RE.match(text_for_match)
        if not m:
            buffer = text_for_match
            continue
        desc = (m.group(1) or "").strip()
        if not desc:
            desc = (buffer or "").strip()
        buffer = ""
        prev, arrec, pmais, pmenos = (
            normalize_number_br_to_float(m.group(2)),
            normalize_number_br_to_float(m.group(3)),
            normalize_number_br_to_float(m.group(4)),
            normalize_number_br_to_float(m.group(5)),
        )
        mc = COD_ROW_RE.match(desc)
        if mc:
            codigo = mc.group(1).upper()
            nome = mc.group(2).strip()
            current_code = codigo
            subitem = ""
            especificacao = nome if nome else codigo
        else:
            if current_code is None:
                continue
            codigo = current_code
            especificacao = ""
            subitem = desc
        rows.append((codigo, especificacao, subitem, prev, arrec, pmais, pmenos))
    df = pd.DataFrame(rows, columns=["codigo","especificacao","subitem","previsao","arrecadacao","para_mais","para_menos"])
    if not df.empty:
        mask_vals = df[["previsao", "arrecadacao", "para_mais", "para_menos"]].notna().any(axis=1)
        df = df[mask_vals].reset_index(drop=True)
    return df

def extract_anexo10_table(pdf_path: Path, verbose: bool = False) -> pd.DataFrame:
    frames: List[pd.DataFrame] = []
    with pdfplumber.open(str(pdf_path)) as pdf:
        for pageno, page in enumerate(pdf.pages, start=1):
            text = page.extract_text(x_tolerance=1, y_tolerance=1) or ""
            lines = [ln for ln in text.splitlines() if ln and ln.strip()]
            df_page = parse_table_lines(lines)
            if verbose:
                print(f"   · página {pageno}: {'ok' if not df_page.empty else 'vazia'}")
            if not df_page.empty:
                frames.append(df_page)
    if not frames:
        return pd.DataFrame(columns=["codigo","especificacao","subitem","previsao","arrecadacao","para_mais","para_menos"])
    df = pd.concat(frames, ignore_index=True)
    # remove linha TOTAL
    def _up(x: pd.Series) -> pd.Series:
        return x.astype(str).str.strip().str.upper()
    mask_total = _up(df.get("codigo", pd.Series("", index=df.index))).eq("TOTAL") \
                 | _up(df.get("especificacao", pd.Series("", index=df.index))).eq("TOTAL")
    df = df.loc[~mask_total].copy()
    df = df.drop_duplicates(subset=["codigo","especificacao","subitem","previsao","arrecadacao","para_mais","para_menos"])
    return df

# ===================== Execução / comparação =====================

def find_raw_csv_for_stage(rawdir: Path, stage: str, ano: int) -> Optional[Path]:
    # padrão baixado pelo 01: raw/<stage>/equiplano_<stage>_anoYYYY.csv
    candidates = list((rawdir / stage).glob(f"*{ano}*.csv"))
    return sorted(candidates)[-1] if candidates else None

def compare_despesas(years: List[int], stages: List[str], rawdir: Path, outdir: Path,
                     base_url: str, timeout: int, retries: int, backoff: float, verbose: bool):
    rows_recon: List[Dict] = []
    cols_log: List[Dict] = []
    outdir.mkdir(parents=True, exist_ok=True)

    with requests.Session() as sess:
        for ano in years:
            for stage in stages:
                raw_csv = find_raw_csv_for_stage(rawdir, stage, ano)
                if raw_csv is None:
                    rows_recon.append({"exercicio": ano, "stage": stage, "raw_total": None, "portal_total": None, "diff_abs": None, "status": "RAW_NAO_ENCONTRADO"})
                    continue

                # RAW
                df_raw = read_csv_any(raw_csv)
                raw_total, raw_used, raw_removed = autodetect_sum_despesa(df_raw, stage)

                # PORTAL snapshot
                snap_csv = fetch_portal_csv(sess, base_url, stage, ano, outdir, timeout, retries, backoff, verbose)
                df_portal = read_csv_any(snap_csv)
                por_total, por_used, por_removed = autodetect_sum_despesa(df_portal, stage)

                rows_recon.append({
                    "exercicio": ano,
                    "stage": stage,
                    "raw_file": str(raw_csv),
                    "portal_file": str(snap_csv),
                    "raw_total": raw_total,
                    "portal_total": por_total,
                    "diff_abs": None if (raw_total is None or por_total is None) else abs(por_total - raw_total),
                })

                cols_log.append({
                    "exercicio": ano, "stage": stage, "lado": "RAW",
                    "arquivo": str(raw_csv), "cols_usadas": ";".join(raw_used), "linhas_total_removidas": raw_removed
                })
                cols_log.append({
                    "exercicio": ano, "stage": stage, "lado": "PORTAL",
                    "arquivo": str(snap_csv), "cols_usadas": ";".join(por_used), "linhas_total_removidas": por_removed
                })

                time.sleep(0.6)

    df_rec = pd.DataFrame(rows_recon)
    df_cols = pd.DataFrame(cols_log)
    df_rec.to_csv(outdir / "D_despesas_reconcile.csv", index=False, encoding="utf-8")
    df_cols.to_csv(outdir / "D_columns_used.csv", index=False, encoding="utf-8")
    return df_rec, df_cols

def compare_receita(years: List[int], rawdir: Path, outdir: Path,
                    timeout: int, retries: int, backoff: float, entidades_arg: Optional[str], verbose: bool):
    rows: List[Dict] = []
    snaps_dir = outdir / "raw_snapshots"
    snaps_dir.mkdir(parents=True, exist_ok=True)

    for ano in years:
        # RAW (saída do 03): raw/receitas/anexo10_prev_arrec_YYYY.csv
        raw_csv = rawdir / "receitas" / f"anexo10_prev_arrec_{ano}.csv"
        if not raw_csv.exists():
            rows.append({"exercicio": ano, "raw_csv": str(raw_csv), "status": "RAW_NAO_ENCONTRADO"})
            continue
        df_raw = pd.read_csv(raw_csv, dtype={"ano":str,"codigo":str,"especificacao":str,"subitem":str}, encoding="utf-8")
        # totals RAW
        prev_raw = pd.to_numeric(df_raw["previsao"], errors="coerce").fillna(0).sum()
        arr_raw  = pd.to_numeric(df_raw["arrecadacao"], errors="coerce").fillna(0).sum()

        # PORTAL snapshot → PDF → parse
        pdf_path = download_anexo10_pdf(ano, snaps_dir, timeout, retries, backoff, entidades_arg, verbose)
        df_por = extract_anexo10_table(pdf_path, verbose=verbose)
        # totals PORTAL
        prev_por = pd.to_numeric(df_por["previsao"], errors="coerce").fillna(0).sum()
        arr_por  = pd.to_numeric(df_por["arrecadacao"], errors="coerce").fillna(0).sum()

        rows.append({
            "exercicio": ano,
            "raw_csv": str(raw_csv),
            "portal_pdf": str(pdf_path),
            "raw_previsao": float(prev_raw),
            "portal_previsao": float(prev_por),
            "diff_previsao": abs(float(prev_por) - float(prev_raw)),
            "raw_arrecadacao": float(arr_raw),
            "portal_arrecadacao": float(arr_por),
            "diff_arrecadacao": abs(float(arr_por) - float(arr_raw)),
            "status": "OK",
        })
        time.sleep(0.8)

    df = pd.DataFrame(rows)
    df.to_csv(outdir / "R_receita_reconcile.csv", index=False, encoding="utf-8")
    return df

def main():
    ap = argparse.ArgumentParser(description="Reconcile entre RAW e Portal (01/02/03).")
    ap.add_argument("--anos", required=True, help="Faixa/lista, ex.: 2018-2025 ou 2018,2019,2020")
    ap.add_argument("--rawdir", default="raw", help="Diretório base do RAW (default: raw)")
    ap.add_argument("--outdir", default="outputs/reconcile_raw_vs_portal", help="Saída (default: outputs/reconcile_raw_vs_portal)")
    ap.add_argument("--base-url", default=CONFIG["base_url"])
    ap.add_argument("--stages", default=",".join(VALID_STAGES), help=f"Stages despesas: {','.join(VALID_STAGES)}")
    ap.add_argument("--include-receita", action="store_true", help="Incluir reconcile da Receita (Anexo 10)")
    ap.add_argument("--entities", help="Códigos de entidades (Anexo 10) ex: 480,482,...; default=todas")
    ap.add_argument("--timeout", type=int, default=90)
    ap.add_argument("--retries", type=int, default=3)
    ap.add_argument("--backoff", type=float, default=1.5)
    ap.add_argument("--verbose", action="store_true")
    args = ap.parse_args()

    years = parse_anos(args.anos)
    rawdir = Path(args.rawdir)
    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    stages = [s.strip() for s in args.stages.split(",") if s.strip()]
    for s in stages:
        if s not in VALID_STAGES:
            print(f"⚠️ Stage inválido: {s}. Válidos: {', '.join(VALID_STAGES)}", file=sys.stderr)
            sys.exit(2)

    if args.verbose:
        print(f"➡️  Reconcile RAW vs PORTAL — anos {years} | stages={stages} | include_receita={args.include_receita}")

    # Despesas (01)
    df_d, df_cols = compare_despesas(
        years, stages, rawdir, outdir,
        args.base_url, args.timeout, args.retries, args.backoff, args.verbose
    )

    # Receita (02+03)
    if args.include_receita:
        df_r = compare_receita(
            years, rawdir, outdir,
            args.timeout, args.retries, args.backoff, args.entities, args.verbose
        )
    else:
        df_r = pd.DataFrame()

    # SUMMARY
    # Considera diffs absolutas >= 1.0 como erro por padrão, só para sinalização básica
    thr = 1.0
    def cnt(df, col):
        return int((pd.to_numeric(df[col], errors="coerce").fillna(0) >= thr).sum()) if (not df.empty and col in df.columns) else 0

    summary_rows = [{
        "anos": f"{years[0]}-{years[-1]}" if len(years)>1 else years[0],
        "desp_diffs_ge_thr": cnt(df_d, "diff_abs"),
        "rec_prev_diffs_ge_thr": cnt(df_r, "diff_previsao"),
        "rec_arr_diffs_ge_thr": cnt(df_r, "diff_arrecadacao"),
        "threshold_abs": thr,
    }]
    pd.DataFrame(summary_rows).to_csv(outdir / "SUMMARY.csv", index=False, encoding="utf-8")

    print("✅ Concluído. Veja relatórios em", outdir)

if __name__ == "__main__":
    main()
