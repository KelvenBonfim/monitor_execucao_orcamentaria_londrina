
import sys
import pytest

# --- helper: safe import ---
def _try_import(modname):
    try:
        __import__(modname)
        return sys.modules[modname]
    except Exception as e:
        pytest.skip(f"could not import {modname}: {e}")

def test_01_helpers_parse_and_regex():
    m = _try_import("01_fetch_equiplano_ano")
    # basic presence of helpers
    for name in ("looks_like_html", "content_is_csv", "DISPLAYTAG_ID_RE", "CSV_ANCHOR_RE", "extract_displaytag_id", "extract_csv_anchor"):
        assert hasattr(m, name)

    # HTML/CSV heuristics
    html = b"<!doctype html><html><body>ok</body></html>"
    assert m.looks_like_html(html) is True
    class _Resp: headers = {"Content-Type":"text/csv"}
    assert m.content_is_csv(_Resp(), b"col1;col2\n1;2\n") is True

    # displaytag id extraction
    assert m.extract_displaytag_id('<div id="d-1234-something">') == "1234"
    # csv anchor extraction
    url = m.extract_csv_anchor('<a href="/foo.csv">CSV</a>', "http://x/base")
    assert url and url.endswith("/foo.csv")

def test_02_payload_and_parse(fake_engine):
    m = _try_import("02_fetch_receita_prev_arrec")
    # anos parser
    assert m.parse_anos("2020-2022") == [2020, 2021, 2022]
    assert m.parse_anos("2020,2022") == [2020, 2022]
    # entidades parser (fallback keeps codes)
    ents = m.parse_entities_arg("480,489")
    assert any(e[0]==480 for e in ents)

    # payload contains required keys
    pld = m.build_payload(2024, ents)
    assert pld["formulario.exercicio"] == "2024"
    assert pld["formulario.tpFormatoExterno"] == "PDF"

def normalize_number_br_to_float(s):
    """
    Converte strings no formato BR para float.
    Exemplos aceitos:
      '1.234,56' -> 1234.56
      '(2.000,00)' -> -2000.0
      '-3.500,00' -> -3500.0
      '–3.500,00' ou '−3.500,00' -> -3500.0  (traço/en-dash/minus unicode)
    """
    import re

    if s is None:
        return 0.0
    s = str(s).strip()

    # Sinal por parênteses contábeis
    neg_paren = s.startswith("(") and s.endswith(")")
    if neg_paren:
        s = s[1:-1].strip()

    # Normaliza traços/minus unicode para '-'
    s = s.replace("–", "-").replace("−", "-")

    # Remove qualquer coisa que não seja dígito, ponto, vírgula ou sinal '-'
    s_clean = re.sub(r"[^0-9\-,\.]", "", s)

    # Converte formato BR: milhares com '.', decimal com ','
    s_clean = s_clean.replace(".", "").replace(",", ".")

    # Se sobrou só sinal ou vazio, zero
    if s_clean in {"", "-"}:
        val = 0.0
    else:
        try:
            val = float(s_clean)
        except ValueError:
            val = 0.0

    if neg_paren:
        val = -abs(val)
    return val

def test_04_utils_only(fake_psycopg2):
    m = _try_import("04_load_csv_to_postgres")
    # utility functions that require no DB
    from pandas import DataFrame
    df = DataFrame({"x":["TOTAL abc","ok"]})
    out = m.strip_total_rows(df)
    assert list(out["x"]) == ["ok"]

    # year inference from filenames
    from pathlib import Path
    assert m.infer_year(Path("raw/receitas/2024-12-31_anexo10.csv")) == 2024

def test_05_parsers_and_sql_helpers():
    m = _try_import("05_build_models")
    assert m.norm("Líquido - Orçamento") == "liquidoorcamento"
    assert m.parse_years("2020-2021") == [2020,2021]
    sql_snippet = m.to_numeric_sql('"valor"')
    assert "REGEXP_REPLACE" in sql_snippet

def test_06_quality_helpers(fake_engine):
    m = _try_import("06_quality_checks")
    assert m.norm_key("Líquido - Orçamento").startswith("liquido")
    assert m.parse_years("2022,2024") == [2022, 2024]
    assert "REGEXP_REPLACE" in m.to_numeric_sql('"v"')

def test_07_backfill_helpers(fake_engine):
    m = _try_import("07_backfill_historico")
    # parse_years requires value
    with pytest.raises(SystemExit):
        m.parse_years(None)
    assert "REGEXP_REPLACE" in m.to_numeric_sql('"v"')

def test_08_reconcile_heuristics():
    m = _try_import("08_reconcile_raw_vs_portal")
    # CSV/HTML heuristics
    class _Resp: headers = {"Content-Type": "application/octet-stream"}
    assert m.content_is_csv(_Resp(), b"col1;col2\n1;2\n") is True
    # regexes
    assert m.extract_displaytag_id('<div id="d-9999">') == "9999"
    url = m.extract_csv_anchor('<a href="file.CSV">baixar</a>', "http://x")
    assert url and url.lower().endswith("file.csv")

def test_09_parse_years_and_sql():
    m = _try_import("09_export_kpis")
    assert m.parse_years_arg("2019-2021") == [2019, 2020, 2021]
    assert m.parse_years_arg("2019,2021") == [2019, 2021]
    assert m.parse_years_arg("2024") == [2024]
