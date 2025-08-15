## 🌐 [Access the Dashboard](https://monitorexecucaoorcamentarialondrina.streamlit.app/)

# Londrina Budget Execution Monitor

This project is a **Budget Execution Monitoring System** for Londrina, allowing analysis of municipal spending and revenue execution over multiple years.  
It supports **two data modes**: CSV-based (local KPIs) and Database mode (Neon-hosted PostgreSQL).

## Running the Pipeline (scripts 01–09)

### 01 — Fetch yearly CSVs (Equiplano / DisplayTag)
Downloads expenses (empenhadas/liquidadas/pagas) CSVs from the legacy portal.
```bash
python scripts/01_fetch_equiplano_ano.py download --anos 2018-2025 --saida raw/ --verbose
# or only some stages
python scripts/01_fetch_equiplano_ano.py download --anos 2024-2025 --stages liquidadas,pagas --saida raw/
# optional: load to staging later with 04 or 05
```

### 02 — Fetch Receita (Anexo 10) — request PDF
Builds the POST payload and fetches the Anexo 10 PDF (per year), no parsing yet.
```bash
python scripts/02_fetch_receita_prev_arrec.py --anos 2018-2025 --out raw/receitas/
```

### 03 — Parse Anexo 10 PDF → CSV
Extracts tables from the Anexo 10 PDF and normalizes BR numbers to floats.
```bash
# melhor resultado: instalar Camelot e Ghostscript
sudo apt install ghostscript
pip install camelot-py[cv] opencv-python

# processar todos os PDFs de receitas_raw/ e salvar um CSV por ano em receitas/
python scripts/03_anexo10_pdf_to_csv.py --in raw/receitas_raw --outdir raw/receitas
```

### 04 — Load CSVs to Postgres (staging)
Loads `raw/` CSVs into `public.stg_*` using `psycopg2/SQLAlchemy`. Requires `DATABASE_URL`.
```bash
python scripts/04_load_csv_to_postgres.py --schema public --staging public --csv raw/
```

### 05 — Build fact tables / models
Creates/refreshes `public.fato_despesa` and `public.fato_receita` + derived summaries.
```bash
python scripts/05_build_models.py --csvdir raw/receitas --schema public --staging public --years 2018-2025 --recreate --verbose
```

### 06 — Quality checks
Cross-validate numeric columns, detect anomalies, and emit reports (CSV/JSON).
```bash
python scripts/06_quality_checks.py --schema public --years 2018-2025 --out outputs/quality/
```

### 07 — Backfill histórico
Recomputes/aligns historical series from staging and facts.
```bash
python scripts/07_backfill_historico.py --schema public --years 2018-2025 --out outputs/backfill/
```

### 08 — Reconcile RAW vs Portal snapshot
Strict reconciliation between current portal snapshot and your RAW.
```bash
python scripts/08_reconcile_raw_vs_portal.py --raw raw/ --out outputs/reconcile_raw_vs_portal/
```

### 09 — Export KPIs (CSV for the app)
Exports yearly KPIs used by the Streamlit app (CSV files in `data/kpis/{YEAR}/`).
```bash
python scripts/09_export_kpis.py --schema public --years 2018-2025 --out data/kpis/
```

## 📂 Project Structure

```
monitor_execucao_orcamentaria_londrina/
├── data/                           # Local data storage
│   └── kpis/                       # Generated KPIs for CSV mode (by year)
│       ├── 2018/ ... 2025/         # One folder per year, containing:
│       │   ├── data_coverage_report.json
│       │   ├── execucao_global_anual.{csv,json}           # Global annual budget execution
│       │   ├── execucao_por_entidade_anual.{csv,json}     # Execution by entity
│       │   ├── receita_prevista_arrecadada_anual.{csv,json} # Revenue forecast vs collected
│       │   ├── superavit_deficit_anual.{csv,json}         # Surplus/deficit summary
│       │   ├── validations_fatos_vs_staging.{csv,json}   # Fact table vs staging validations
│       │   └── (other validations if generated)
├── logs/                           # Execution log files
│   ├── collect_project_snapshot.log
│   └── load_*.log
├── londrina_dir/                   # PostgreSQL directory-format dump (pg_dump -F d)
│   ├── *.dat.gz
│   └── toc.dat
├── outputs/                        # Processed outputs
│   ├── quality/                    # Data quality analysis results
│   │   ├── R1_inequalities.csv
│   │   ├── R4_reconcile_fatos_vs_staging.csv
│   │   ├── R6_yoy_anomalias.csv
│   │   └── SUMMARY.csv
│   ├── quality_checks/             # Automated quality check outputs
│   │   ├── R1_inequalities.csv
│   │   ├── R4_reconcile_fatos_vs_staging.csv
│   │   └── R6_yoy_anomalias.csv
│   └── reconcile_raw_vs_portal/    # Reconciliation between raw and portal data
│       └── raw_snapshots/          # Raw yearly snapshots for reconciliation
│           ├── 2018/ ... 2025/     # One folder per year, containing:
│           │   ├── equiplano_empenhadas_anoYYYY.csv
│           │   ├── equiplano_liquidadas_anoYYYY.csv
│           │   └── equiplano_pagas_anoYYYY.csv
├── raw/                            # Collected raw datasets
│   ├── empenhadas/                 # Commitments per year (Equiplano)
│   ├── liquidadas/                 # Liquidations per year (Equiplano)
│   ├── pagas/                      # Payments per year (Equiplano)
│   └── receitas_raw/               # Raw revenue data from Anexo 10
│       ├── _html_debug/            # Optional debug files
│       └── *.pdf                   # Original Anexo 10 PDFs
├── scripts/                        # Automation and ETL scripts
│   ├── 01_fetch_equiplano_ano.py       # Downloads annual expense CSVs from Equiplano
│   ├── 02_fetch_receita_prev_arrec.py  # Downloads forecast & collected revenue data
│   ├── 03_anexo10_pdf_to_csv.py        # Converts Anexo 10 PDF to CSV
│   ├── 04_load_csv_to_postgres.py      # Loads CSV files into PostgreSQL
│   ├── 05_build_models.py              # Builds staging and fact tables in PostgreSQL
│   ├── 06_quality_checks.py            # Runs data quality checks and exports reports
│   ├── 07_backfill_historico.py        # Backfills historical data into the database
│   ├── 08_reconcile_raw_vs_portal.py   # Performs strict reconciliation with the portal
│   └── 09_export_kpis.py               # Generates KPI files for CSV mode
├─ tests/
│   ├─ conftest.py
│   └─ test_scripts.py
├── app.py                          # Streamlit application for dashboard visualization
├── LICENCE                         # License file (MIT)
├── README.md                       # Main documentation (English)
├── README.pt-br.md                 # Documentation in Portuguese
└── requirements.txt                # Python dependencies
```

## ⚙️ Installation

1. **Clone the repository**
```bash
git clone https://github.com/yourusername/londrina-budget-monitor.git
cd londrina-budget-monitor
```

2. **Create and activate a virtual environment**
```bash
python3 -m venv venv
source venv/bin/activate  # Linux/Mac
venv\Scripts\activate   # Windows
```

3. **Install dependencies**
```bash
pip install -r requirements.txt
```

## 🗄️ Data Modes

The project can work in two modes:

### **1. CSV Mode (Local Data)**
- KPIs are pre-generated and stored in `data/kpis/<year>/`.
- If KPIs are missing, run:
```bash
python scripts/09_export_kpis.py
git add data/kpis/
git commit -m "Add generated KPIs"
```

### **2. Database Mode (Neon-hosted PostgreSQL)**
- The dashboard fetches KPI data directly from the online database.
- Configure your `.env` file with:
```
DATABASE_URL=postgresql+psycopg2://user:password@host/dbname
```

## 📊 Scripts Overview

| Script | Description |
|--------|-------------|
| `01_fetch_equiplano_ano.py` | Downloads CSVs from the Equiplano portal for **Empenhadas, Liquidadas, Pagas**. |
| `02_fetch_receita_prev_arrec.py` | Downloads projected and collected revenue data. |
| `03_anexo10_pdf_to_csv.py` | Parses Anexo 10 PDF into CSV format. |
| `04_load_csv_to_postgres.py` | Loads processed CSV data into PostgreSQL tables. |
| `05_build_models.py` | Builds fact tables in PostgreSQL for staging and production schemas. |
| `06_quality_checks.py` | Runs automated data quality checks on processed datasets. |
| `07_backfill_historico.py` | Backfills historical budget data for years prior to the main dataset. |
| `08_reconcile_raw_vs_portal.py` | Compares RAW stored data against the current transparency portal data. |
| `09_export_kpis.py` | Generates annual KPI CSV files for dashboard consumption. |

## 🚀 Running the Dashboard (Streamlit)

1. Ensure data is available (CSV Mode) or DB is connected (Database Mode).
2. Start Streamlit:
```bash
streamlit run dashboard/app.py
```

## 🌐 Deploying with Neon Database

1. Create a database on **Neon.tech**.
2. Import staging tables using:
```bash
python scripts/05_build_models.py --schema public --staging public --years 2018-2025 --recreate --verbose
```
3. Update `.env` with Neon connection string.

## 📈 KPI Files

In CSV mode, the following KPI files are required in `data/kpis/<year>/`:
- `execucao_por_funcao_anual.csv`
- `execucao_por_orgao_unidade_anual.csv`

If not present, run:
```bash
python scripts/09_export_kpis.py
```

## 📝 Notes

- Use **CSV mode** for offline testing and reproducibility.
- Use **DB mode** for production with live updates.
- Always commit KPI files in CSV mode for version tracking.

---

**Author:** Kelven de Alcantara Bonfim  
**License:** MIT
