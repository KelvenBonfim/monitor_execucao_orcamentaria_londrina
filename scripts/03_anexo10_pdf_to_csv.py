#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Extrai a tabela do Anexo 10 (Receita Prevista x Arrecadada) dos PDFs em raw/receitas_raw/
e gera um CSV POR ANO em raw/receitas/anexo10_prev_arrec_<ANO>.csv.

Robustez:
- Detecta cabeçalho "CÓDIGO ESPECIFICAÇÃO ..." com casefold (maiúsculas/minúsculas/acentos).
- Varrimento por TODAS as páginas até achar a tabela.
- Junta descrições quebradas em várias linhas; diferencia linhas de código e subitens.
- Normaliza números BR em float.

Uso:
  python scripts/03_anexo10_pdf_to_csv.py \
    --pdf "raw/receitas_raw/*.pdf" \
    --outdir "raw/receitas" \
    --verbose --ptbr

Requisitos:
  pip install pdfplumber pandas
"""

import argparse
import glob
import re
from pathlib import Path
from typing import List, Optional, Tuple

import pandas as pd
import pdfplumber

# --------------------------- utilidades ---------------------------

NUM_BR = r"-?\(?\s*(?:\d{1,3}(?:\.\d{3})*|\d+),\d{2}\s*\)?"
ROW_TAIL_RE = re.compile(
    rf"(.*?)\s+({NUM_BR})\s+({NUM_BR})\s+({NUM_BR})\s+({NUM_BR})\s*$"
)

# linha principal começa por TOTAL ou por código numérico de 1 ou 2 dígitos
COD_ROW_RE = re.compile(r"^\s*(TOTAL|\d{1,2})\b\s*(.*)$", re.IGNORECASE)


def infer_ano_from_name(p: Path) -> Optional[int]:
    m = re.search(r"(\d{4})[-_]12[-_]31", p.name)
    if m:
        return int(m.group(1))
    m2 = re.search(r"(\d{4})", p.name)
    return int(m2.group(1)) if m2 else None


def looks_like_header(line: str) -> bool:
    s = line.casefold()
    return any(
        key in s
        for key in (
            "consolidação geral",
            "consolidacao geral",
            "anexo 10",
            "página",
            "pagina",
            "conjunto de informações",
            "entidades consolidadas",
        )
    )


def is_columns_header(line: str) -> bool:
    s = line.casefold()
    return "código" in s and "especificação" in s


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
    """
    Recebe as linhas de texto de UMA página e retorna DataFrame com:
    codigo, especificacao, subitem, previsao, arrecadacao, para_mais, para_menos
    ou DataFrame vazio se não encontrou uma tabela válida nesta página.
    """
    # 1) localizar o cabeçalho das colunas
    start_idx = None
    for i, ln in enumerate(lines):
        if is_columns_header(ln):
            start_idx = i + 1
            break
    if start_idx is None:
        return pd.DataFrame(columns=[
            "codigo","especificacao","subitem",
            "previsao","arrecadacao","para_mais","para_menos"
        ])

    # 2) varrer acumulando descrições até pegar os 4 números finais
    rows: List[Tuple[str, str, str, float, float, float, float]] = []
    current_code: Optional[str] = None
    buffer = ""

    for raw in lines[start_idx:]:
        line = raw.strip()
        if not line:
            continue
        if looks_like_header(line):  # outro cabeçalho/rodapé
            break

        text_for_match = (buffer + " " + line).strip() if buffer else line
        m = ROW_TAIL_RE.match(text_for_match)
        if not m:
            buffer = text_for_match
            continue

        # temos uma linha completa: descrição + 4 números
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
                # se ainda não achamos um bloco com código, ignora por segurança
                continue
            codigo = current_code
            especificacao = ""
            subitem = desc

        rows.append((codigo, especificacao, subitem, prev, arrec, pmais, pmenos))

    df = pd.DataFrame(
        rows,
        columns=[
            "codigo",
            "especificacao",
            "subitem",
            "previsao",
            "arrecadacao",
            "para_mais",
            "para_menos",
        ],
    )
    if not df.empty:
        mask_vals = df[["previsao", "arrecadacao", "para_mais", "para_menos"]].notna().any(axis=1)
        df = df[mask_vals].reset_index(drop=True)
    return df


def extract_table_from_pdf(pdf_path: Path, verbose: bool = False) -> pd.DataFrame:
    frames: List[pd.DataFrame] = []

    with pdfplumber.open(str(pdf_path)) as pdf:
        for pageno, page in enumerate(pdf.pages, start=1):
            text = page.extract_text(x_tolerance=1, y_tolerance=1) or ""
            lines = [ln for ln in text.splitlines() if ln and ln.strip()]
            df_page = parse_table_lines(lines)
            if verbose:
                print(f"   · página {pageno}: {'ok' if not df_page.empty else 'vazia'} ({len(lines)} linhas extraídas)")
            if not df_page.empty:
                frames.append(df_page)

    if not frames:
        return pd.DataFrame(columns=[
            "codigo","especificacao","subitem",
            "previsao","arrecadacao","para_mais","para_menos"
        ])

    df = pd.concat(frames, ignore_index=True)

    # --- limpeza final defensiva ---
    def _up(x: pd.Series) -> pd.Series:
        return x.astype(str).str.strip().str.upper()

    # remove linha TOTAL (alguns PDFs repetem TOTAL em outra página)
    mask_total = _up(df.get("codigo", pd.Series("", index=df.index))).eq("TOTAL") \
                 | _up(df.get("especificacao", pd.Series("", index=df.index))).eq("TOTAL")
    df = df.loc[~mask_total].copy()

    # trim textos
    for c in ("codigo", "especificacao", "subitem"):
        if c in df.columns:
            df[c] = df[c].astype(str).str.strip()

    # remove duplicatas exatas (pode ocorrer em quebras de página)
    df = df.drop_duplicates(subset=["codigo", "especificacao", "subitem", "previsao", "arrecadacao", "para_mais", "para_menos"])

    # garante ordem de colunas
    df = df[["codigo","especificacao","subitem","previsao","arrecadacao","para_mais","para_menos"]]

    return df


# --------------------------- execução ---------------------------

def build_arg_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(description="Extrai a tabela do Anexo 10 (PDF) e gera um CSV por ano.")
    ap.add_argument("--pdf", required=True, help='Glob de PDFs. Ex.: "raw/receitas_raw/*.pdf"')
    ap.add_argument("--outdir", default="raw/receitas", help='Diretório de saída (default: raw/receitas)')
    ap.add_argument("--ptbr", action="store_true", help="Salvar com separador ';' e vírgula decimal")
    ap.add_argument("--limit", type=int, help="Processar no máximo N arquivos (debug)")
    ap.add_argument("--verbose", action="store_true")
    return ap


def save_csv(df: pd.DataFrame, out_path: Path, ptbr: bool):
    out_path.parent.mkdir(parents=True, exist_ok=True)
    if ptbr:
        # converte floats -> string PT-BR e usa ; como separador
        df2 = df.copy()
        num_cols = ["previsao", "arrecadacao", "para_mais", "para_menos"]
        for c in num_cols:
            df2[c] = df2[c].map(lambda v: (f"{v:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")) if pd.notna(v) else "")
        df2.to_csv(out_path, index=False, sep=";", encoding="utf-8-sig")
    else:
        df.to_csv(out_path, index=False, encoding="utf-8")


def main():
    ap = build_arg_parser()
    args = ap.parse_args()

    files = sorted(glob.glob(args.pdf))
    if args.limit:
        files = files[: args.limit]
    if not files:
        raise SystemExit("Nenhum PDF encontrado pelo padrão informado.")

    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    for pdf_file in files:
        p = Path(pdf_file)
        year = infer_ano_from_name(p)
        if year is None:
            print(f"⚠️  Ignorando (não deu para inferir ano do nome): {p.name}")
            continue

        if args.verbose:
            print(f"🔎 {p.name} — procurando tabela em todas as páginas...")

        try:
            df = extract_table_from_pdf(p, verbose=args.verbose)
            if df.empty:
                print(f"❌ {p.name}: não foi possível localizar a tabela.")
                continue

            # injeta ano e reordena
            df.insert(0, "ano", year)
            df = df[["ano", "codigo", "especificacao", "subitem", "previsao", "arrecadacao", "para_mais", "para_menos"]]

            out_csv = outdir / f"anexo10_prev_arrec_{year}.csv"
            save_csv(df, out_csv, ptbr=args.ptbr)
            print(f"✅ {year}: salvo {out_csv} ({len(df)} linhas)")
        except Exception as e:
            print(f"❌ {p.name}: {e}")


if __name__ == "__main__":
    main()
