#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
03_anexo10_pdf_to_csv.py
Extrai a tabela do Anexo 10 (Receita Prevista x Arrecadada) de PDFs e gera CSV(s).

Estratégia (robusta p/ seus PDFs):
- Usa pdfplumber para extrair TEXTO da página inteira.
- Encontra o cabeçalho de colunas ("CÓDIGO", "ESPECIFICAÇÃO"...).
- Junta descrições quebradas em múltiplas linhas até capturar os 4 valores numéricos finais.
- Diferencia linhas "de código" (TOTAL ou 1–2 dígitos) de subitens.
- Normaliza números pt-BR para float.
- Remove rodapés e linhas TOTAL duplicadas.

CLI:
- Aceita --in (pasta) ou --pdf (glob/arquivo). Um dos dois é obrigatório.
- Para saída, use --outdir (um CSV por PDF). Opcionalmente, --ptbr para CSV com ; e vírgula decimal.
- Também funciona no modo "concat" com --out (CSV único), se preferir.

Exemplos:
  # Vários PDFs em raw/receitas_raw -> CSV por ano em raw/receitas
  python scripts/03_anexo10_pdf_to_csv.py --in raw/receitas_raw --outdir raw/receitas --verbose

  # Glob explícito (mesmo comportamento do seu script antigo)
  python scripts/03_anexo10_pdf_to_csv.py --pdf "raw/receitas_raw/*.pdf" --outdir raw/receitas

Requisitos:
  pip install pdfplumber pandas
"""

from __future__ import annotations
import argparse, glob, re, sys
from pathlib import Path
from typing import List, Optional, Tuple, Iterable
import pandas as pd
import pdfplumber

# --------------------------- utilidades ---------------------------

# número BR: 1.234.567,89 (aceita parênteses p/ negativos)
NUM_BR = r"-?\(?\s*(?:\d{1,3}(?:\.\d{3})*|\d+),\d{2}\s*\)?"

# linha termina com 4 números (prev, arrec, pmais, pmenos)
ROW_TAIL_RE = re.compile(rf"(.*?)\s+({NUM_BR})\s+({NUM_BR})\s+({NUM_BR})\s+({NUM_BR})\s*$")

# linha "principal" começa por TOTAL OU por código numérico de 1 ou 2 dígitos
COD_ROW_RE = re.compile(r"^\s*(TOTAL|\d{1,2})\b\s*(.*)$", re.IGNORECASE)

HEADER_NOISY_RE = re.compile(
    r"(consolida[cç][aã]o geral|anexo\s*10|p[aá]gina|conjunto de informa[cç][oõ]es|entidades consolidadas)",
    flags=re.IGNORECASE
)

def log(msg: str) -> None:
    print(f"[03_anexo10] {msg}", file=sys.stderr)

def looks_like_header(line: str) -> bool:
    return bool(HEADER_NOISY_RE.search(line))

def is_columns_header(line: str) -> bool:
    s = line.casefold()
    return ("código" in s or "codigo" in s) and "especifica" in s  # cobre especificação/especificacao

def normalize_number_br_to_float(txt: str) -> Optional[float]:
    if txt is None:
        return None
    s = str(txt).strip()
    if s.startswith("(") and s.endswith(")"):
        s = s[1:-1]
    s = s.replace("\xa0", "").replace(" ", "").replace(".", "")
    s = s.replace(",", ".")
    s = re.sub(r"[^0-9.\-]", "", s)
    if s in ("", "-", ".", "-.", ".-"):
        return None
    try:
        return float(s)
    except Exception:
        return None

def infer_year_from_name(p: Path) -> Optional[int]:
    m = re.search(r"(\d{4})[-_]12[-_]31", p.name)
    if m:
        return int(m.group(1))
    m2 = re.search(r"(19|20)\d{2}", p.name)
    return int(m2.group(0)) if m2 else None

# --------------------------- parsing de página ---------------------------

def parse_table_lines(lines: List[str]) -> pd.DataFrame:
    """
    Recebe as linhas de texto de UMA página e retorna DataFrame com:
    codigo, especificacao, subitem, previsao, arrecadacao, para_mais, para_menos
    (ou DF vazio se a página não contém a tabela).
    """
    # 1) localizar o cabeçalho das colunas
    start_idx = None
    for i, ln in enumerate(lines):
        if is_columns_header(ln):
            start_idx = i + 1
            break
    if start_idx is None:
        return pd.DataFrame(columns=[
            "codigo","especificacao","subitem","previsao","arrecadacao","para_mais","para_menos"
        ])

    # 2) varrer acumulando descrições até pegar os 4 números finais
    rows: List[Tuple[str, str, str, float, float, float, float]] = []
    current_code: Optional[str] = None
    buffer = ""

    for raw in lines[start_idx:]:
        line = raw.strip()
        if not line:
            continue
        if looks_like_header(line):  # outro cabeçalho/rodapé delimitando a área
            break

        text_for_match = (buffer + " " + line).strip() if buffer else line
        m = ROW_TAIL_RE.match(text_for_match)
        if not m:
            buffer = text_for_match
            continue

        # Temos uma linha completa: descrição + 4 números
        desc = (m.group(1) or "").strip()
        if not desc:
            desc = (buffer or "").strip()
        buffer = ""

        prev  = normalize_number_br_to_float(m.group(2))
        arrec = normalize_number_br_to_float(m.group(3))
        pmais = normalize_number_br_to_float(m.group(4))
        pmenos= normalize_number_br_to_float(m.group(5))

        mc = COD_ROW_RE.match(desc)
        if mc:
            codigo = mc.group(1).upper()
            nome = (mc.group(2) or "").strip()
            current_code = codigo
            subitem = ""
            especificacao = nome if nome else codigo
        else:
            if current_code is None:
                # ainda não encontrou bloco com código → ignora por segurança
                continue
            codigo = current_code
            especificacao = ""
            subitem = desc

        rows.append((codigo, especificacao, subitem, prev, arrec, pmais, pmenos))

    df = pd.DataFrame(rows, columns=[
        "codigo","especificacao","subitem","previsao","arrecadacao","para_mais","para_menos"
    ])

    if not df.empty:
        # manter apenas linhas com algum valor numérico
        mask_vals = df[["previsao","arrecadacao","para_mais","para_menos"]].notna().any(axis=1)
        df = df[mask_vals].reset_index(drop=True)
    return df

# --------------------------- extração do PDF ---------------------------

def extract_table_from_pdf(pdf_path: Path, verbose: bool = False) -> pd.DataFrame:
    frames: List[pd.DataFrame] = []

    with pdfplumber.open(str(pdf_path)) as pdf:
        for pageno, page in enumerate(pdf.pages, start=1):
            txt = page.extract_text(x_tolerance=1, y_tolerance=1) or ""
            lines = [ln for ln in txt.splitlines() if ln and ln.strip()]
            df_page = parse_table_lines(lines)
            if verbose:
                log(f"   · página {pageno}: {'ok' if not df_page.empty else 'vazia'} ({len(lines)} linhas)")
            if not df_page.empty:
                frames.append(df_page)

    if not frames:
        return pd.DataFrame(columns=[
            "codigo","especificacao","subitem","previsao","arrecadacao","para_mais","para_menos"
        ])

    df = pd.concat(frames, ignore_index=True)

    # limpeza final defensiva
    def _up(series: pd.Series) -> pd.Series:
        return series.astype(str).str.strip().str.upper()

    mask_total = _up(df.get("codigo", pd.Series("", index=df.index))).eq("TOTAL") \
               | _up(df.get("especificacao", pd.Series("", index=df.index))).eq("TOTAL")
    df = df.loc[~mask_total].copy()

    for c in ("codigo","especificacao","subitem"):
        df[c] = df[c].astype(str).str.strip()

    df = df.drop_duplicates(subset=["codigo","especificacao","subitem","previsao","arrecadacao","para_mais","para_menos"])
    df = df[["codigo","especificacao","subitem","previsao","arrecadacao","para_mais","para_menos"]]
    return df.reset_index(drop=True)

# --------------------------- IO / CLI ---------------------------

def iter_pdf_paths_from_cli(in_dir: Optional[Path], pdf_arg: Optional[str]) -> List[Path]:
    if in_dir:
        return sorted(list(in_dir.glob("*.pdf")) + list(in_dir.glob("*.PDF")))
    if pdf_arg:
        return [Path(p) for p in sorted(glob.glob(pdf_arg))]
    return []

def save_csv(df: pd.DataFrame, out_path: Path, ptbr: bool):
    out_path.parent.mkdir(parents=True, exist_ok=True)
    if ptbr:
        # formata números no padrão PT-BR e usa ';'
        df2 = df.copy()
        for c in ["previsao","arrecadacao","para_mais","para_menos"]:
            df2[c] = df2[c].map(lambda v: (f"{v:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")) if pd.notna(v) else "")
        df2.to_csv(out_path, index=False, sep=";", encoding="utf-8-sig")
    else:
        df.to_csv(out_path, index=False, encoding="utf-8")

def main():
    ap = argparse.ArgumentParser(description="Extrai a tabela do Anexo 10 (PDF) e gera CSV(s).")
    mex_in = ap.add_mutually_exclusive_group(required=True)
    mex_in.add_argument("--in", dest="indir", help="Pasta com PDFs (ex.: raw/receitas_raw)")
    mex_in.add_argument("--pdf", dest="pdfglob", help='Glob/arquivo (ex.: "raw/receitas_raw/*.pdf")')

    mex_out = ap.add_mutually_exclusive_group(required=True)
    mex_out.add_argument("--outdir", help="Diretório de saída (1 CSV por PDF).")
    mex_out.add_argument("--out", help="CSV único concatenado.")

    ap.add_argument("--ptbr", action="store_true", help="Salvar CSV em PT-BR (; e vírgula decimal).")
    ap.add_argument("--verbose", action="store_true")
    ap.add_argument("--limit", type=int, help="Processar no máx. N arquivos (debug).")
    args = ap.parse_args()

    in_dir = Path(args.indir) if args.indir else None
    pdfs = iter_pdf_paths_from_cli(in_dir, args.pdfglob)
    if args.limit:
        pdfs = pdfs[: args.limit]
    if not pdfs:
        raise SystemExit("Nenhum PDF encontrado.")

    if args.outdir:
        outdir = Path(args.outdir)
        outdir.mkdir(parents=True, exist_ok=True)

        for p in pdfs:
            year = infer_year_from_name(p)
            if year is None:
                log(f"⚠️  Ignorando (não deu para inferir ano): {p.name}")
                continue

            log(f"Processando: {p.name}")
            df = extract_table_from_pdf(p, verbose=args.verbose)
            if df.empty:
                log(f"Aviso: não foi possível localizar a tabela em {p.name}.")
                # salva vazio para marcar tentativa (opcional: pular)
                out_csv = outdir / f"anexo10_prev_arrec_{year}.csv"
                save_csv(df, out_csv, ptbr=args.ptbr)
                continue

            df.insert(0, "ano", year)
            df = df[["ano","codigo","especificacao","subitem","previsao","arrecadacao","para_mais","para_menos"]]
            out_csv = outdir / f"anexo10_prev_arrec_{year}.csv"
            save_csv(df, out_csv, ptbr=args.ptbr)
            log(f"CSV salvo: {out_csv} (linhas: {len(df)})")
    else:
        out_path = Path(args.out)
        out_path.parent.mkdir(parents=True, exist_ok=True)

        all_frames: List[pd.DataFrame] = []
        for p in pdfs:
            year = infer_year_from_name(p)
            if year is None:
                log(f"⚠️  Ignorando (não deu para inferir ano): {p.name}")
                continue

            log(f"Processando: {p.name}")
            df = extract_table_from_pdf(p, verbose=args.verbose)
            if df.empty:
                log(f"Aviso: não foi possível localizar a tabela em {p.name}.")
                continue
            df.insert(0, "ano", year)
            all_frames.append(df)

        if not all_frames:
            raise SystemExit("Nenhuma tabela extraída para concatenar.")

        final = pd.concat(all_frames, ignore_index=True)
        final = final[["ano","codigo","especificacao","subitem","previsao","arrecadacao","para_mais","para_menos"]]
        save_csv(final, out_path, ptbr=args.ptbr)
        log(f"CSV único salvo: {out_path} (linhas: {len(final)})")

if __name__ == "__main__":
    main()
