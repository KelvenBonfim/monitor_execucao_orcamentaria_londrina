#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Baixa o PDF do Anexo 10 (Comparativo da Receita Prevista x Arrecadada) do portal
e salva em raw/receitas_raw/<YYYY-12-31>_anexo10_prev_arrec.pdf (por ano).

Uso:
  # Baixar 2018–2025
  python scripts/02_fetch_receita_prev_arrec.py download --anos 2018-2025

  # Baixar anos específicos
  python scripts/02_fetch_receita_prev_arrec.py download --anos 2018,2020,2025

  # Com mais tolerância de rede:
  python scripts/02_fetch_receita_prev_arrec.py download --anos 2018-2025 --timeout 240 --retries 8 --backoff 4 --verbose

Requisitos: requests
"""

import argparse
import random
import sys
import time
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import requests

BASE = "http://portaltransparencia.londrina.pr.gov.br:8080"
URL_PROCESS = f"{BASE}/transparencia/execucaoOrcamentariaAnexo10ComparativoDaReceitaPrevistaComArrecadada/process"

# Lista padrão (todas)
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
    return [int(x) for x in s.replace(";", ",").split(",") if x.strip()]


def parse_entities_arg(arg: Optional[str]) -> List[Tuple[int, str, str]]:
    if not arg:
        return ENTIDADES_DEFAULT
    wanted = {int(x.strip()) for x in arg.split(",") if x.strip()}
    lookup = {cod: (cod, nome, tipo) for cod, nome, tipo in ENTIDADES_DEFAULT}
    out: List[Tuple[int, str, str]] = []
    for cod in sorted(wanted):
        out.append(lookup.get(cod, (cod, f"Entidade {cod}", "AUTARQUIA")))
    return out


def build_payload(year: int, entidades: List[Tuple[int, str, str]]) -> Dict[str, str]:
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


def is_html(content: bytes) -> bool:
    head = (content or b"")[:2048].lstrip().lower()
    return head.startswith(b"<!doctype") or head.startswith(b"<html")


def looks_like_pdf(content: bytes, content_type: str) -> bool:
    if content.startswith(b"%PDF"):
        return True
    ct = (content_type or "").lower()
    return ("application/pdf" in ct) or ("octet-stream" in ct and not is_html(content))


def download_year(year: int, out_dir: Path, timeout: int, retries: int, backoff: float, entidades_arg: Optional[str], verbose: bool) -> bool:
    entidades = parse_entities_arg(entidades_arg)
    payload = build_payload(year, entidades)

    s = requests.Session()
    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "Origin": BASE,
        "Referer": f"{BASE}/transparencia/execucaoOrcamentariaAnexo10ComparativoDaReceitaPrevistaComArrecadada",
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36",
        "Accept": "application/pdf,application/octet-stream,*/*;q=0.8",
        "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.7,en;q=0.5",
        "Connection": "close",
    }

    out_dir.mkdir(parents=True, exist_ok=True)
    out_pdf = out_dir / f"{year}-12-31_anexo10_prev_arrec.pdf"
    debug_dir = out_dir / "_html_debug"
    debug_dir.mkdir(parents=True, exist_ok=True)

    last_err: Optional[Exception] = None
    for attempt in range(1, retries + 1):
        try:
            if verbose:
                print(f"→ {year}: tentativa {attempt}/{retries}…")
            r = s.post(URL_PROCESS, data=payload, headers=headers, timeout=timeout)
            content = r.content or b""
            ctype = r.headers.get("Content-Type", "")

            if is_html(content):
                dbg = debug_dir / f"{year}_attempt{attempt}.html"
                dbg.write_bytes(content)
                raise RuntimeError(f"Servidor retornou HTML (sessão/params). Debug: {dbg}")

            if not looks_like_pdf(content, ctype):
                dbg = debug_dir / f"{year}_attempt{attempt}_nao_pdf.bin"
                dbg.write_bytes(content)
                print(f"⚠️ {year}: conteúdo não parece PDF; salvando assim mesmo (parser pode falhar).")

            out_pdf.write_bytes(content)
            if verbose:
                print(f"✅ {year}: salvo {out_pdf} ({len(content)} bytes)")
            return True

        except Exception as e:
            last_err = e
            print(f"⚠️ {year}: tentativa {attempt}/{retries} falhou: {e}")
            sleep_s = max(1.5, backoff * attempt) + random.uniform(0, 1.2)
            time.sleep(sleep_s)

    print(f"❌ {year}: falhou após {retries} tentativas. Último erro: {last_err}", file=sys.stderr)
    return False


def cmd_download(args) -> None:
    years = parse_anos(args.anos)
    out_dir = Path(args.saida)
    ok_all = True
    for y in years:
        ok = download_year(
            year=y,
            out_dir=out_dir,
            timeout=args.timeout,
            retries=args.retries,
            backoff=args.backoff,
            entidades_arg=args.entities,
            verbose=args.verbose,
        )
        ok_all = ok_all and ok
        # pequena pausa entre anos para aliviar o servidor
        time.sleep(1.5)
    if not ok_all:
        sys.exit(2)


def cmd_load(_args) -> None:
    # Placeholder para manter compatibilidade da assinatura {download,load}
    print("Modo 'load' não implementado para o Anexo 10 (PDF). Use apenas o subcomando 'download'.")
    sys.exit(2)


def build_arg_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(description="Baixa PDFs do Anexo 10 (Receita Prevista x Arrecadada).")
    sub = ap.add_subparsers(dest="mode", required=True)

    dl = sub.add_parser("download", help="Baixa PDFs por faixa/lista de anos.")
    dl.add_argument("--anos", required=True, help="Ex.: 2018-2025 ou 2018,2020,2025")
    dl.add_argument("--entities", help="Códigos de entidades separados por vírgula (ex.: 480,482,483). Se omitido: todas.")
    dl.add_argument("--saida", default="raw/receitas_raw", help="Diretório de saída (default: raw/receitas_raw)")
    dl.add_argument("--timeout", type=int, default=180, help="Timeout por tentativa, em segundos (default: 180)")
    dl.add_argument("--retries", type=int, default=6, help="Número de tentativas com backoff (default: 6)")
    dl.add_argument("--backoff", type=float, default=2.0, help="Multiplicador de backoff entre tentativas (default: 2.0)")
    dl.add_argument("--verbose", action="store_true")
    dl.set_defaults(func=cmd_download)

    ld = sub.add_parser("load", help="(não implementado) placeholder para compatibilidade.")
    ld.set_defaults(func=cmd_load)

    return ap


def main():
    ap = build_arg_parser()
    args = ap.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
