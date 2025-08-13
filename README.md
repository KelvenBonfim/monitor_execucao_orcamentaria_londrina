# Londrina Budget Execution Monitor

Project for collecting, analyzing, reconciling, and visualizing budget data from the municipality of Londrina-PR, Brazil.

## Objective
Automate the download, processing, and comparison of budget data on expenses and revenues between historical sources (RAW) and the current transparency portal, with visualization through a public dashboard.

## Project Structure

```
monitor_execucao_orcamentaria_londrina/
├── logs/                          # Execution log files
│   ├── collect_project_snapshot.log
│   └── load_*.log
├── outputs/                       # Processed outputs
│   ├── kpis/                      # Annual indicators (CSV + JSON)
│   │   ├── 2018/ ... 2025/         # One folder per year, containing:
│   │   │   ├── data_coverage_report.json
│   │   │   ├── execucao_global_anual.{csv,json}
│   │   │   ├── execucao_por_entidade_anual.{csv,json}
│   │   │   ├── receita_prevista_arrecadada_anual.{csv,json}
│   │   │   ├── superavit_deficit_anual.{csv,json}
│   │   │   ├── validations_fatos_vs_staging.{csv,json}
│   │   │   └── validations_staging_vs_raw.{csv,json}
│   ├── quality/                   # Data quality reports
│   ├── quality_checks/            # Automated checks (CSV)
│   └── reconcile_raw_vs_portal/   # Data reconciliation with the portal
│       └── raw_snapshots/         # Raw yearly snapshots (CSV)
├── raw/                           # Collected raw data
│   ├── empenhadas/                # Commitment CSVs per year
│   ├── liquidadas/                # Liquidated expenses per year
│   ├── pagas/                     # Paid expenses per year
│   └── receitas/                  # Revenues from Anexo 10 (PDF converted to CSV)
├── scripts/                       # Automation scripts
│   ├── 01_fetch_equiplano_ano.py  # Download expense CSVs (Equiplano)
│   ├── 02_fetch_receita_prev_arrec.py  # Download forecast & collected revenue (Anexo 10)
│   ├── 03_anexo10_pdf_to_csv.py   # Convert Anexo 10 PDF to CSV
│   ├── 04_quality_checks.py       # Data quality checks
│   ├── 05_build_models.py         # Builds staging & facts tables in PostgreSQL
│   ├── 06_kpi_generator.py        # Generates KPIs
│   └── 08_reconcile_raw_vs_portal.py  # Strict reconciliation
└── tools/                         # SQL and utility files
    └── sql/                       # SQL scripts for the database
```

## Requirements
- Python 3.10+
- PostgreSQL 14+
- Required Python packages: pandas, requests, psycopg2, reportlab

## How to Run

### 1. Set up environment
```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 2. Download data
```bash
python scripts/01_fetch_equiplano_ano.py download --anos 2018-2025 --saida raw/
```

### 3. Process revenues (Anexo 10)
```bash
python scripts/02_fetch_receita_prev_arrec.py download --anos 2018-2025 --saida raw/
python scripts/03_anexo10_pdf_to_csv.py --input raw/receitas/2025.pdf --output raw/receitas/2025.csv
```

### 4. Run quality checks
```bash
python scripts/04_quality_checks.py
```

### 5. Build database models
```bash
python scripts/05_build_models.py --schema public --staging public --years 2018-2025 --recreate
```

### 6. Generate KPIs
```bash
python scripts/06_kpi_generator.py
```

### 7. Reconcile RAW vs Portal
```bash
python scripts/08_reconcile_raw_vs_portal.py --outdir outputs/reconcile_raw_vs_portal
```

## License
MIT License
