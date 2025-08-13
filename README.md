# Londrina Budget Execution Monitor

This project is a **Budget Execution Monitoring System** for Londrina, allowing analysis of municipal spending and revenue execution over multiple years.  
It supports **two data modes**: CSV-based (local KPIs) and Database mode (Neon-hosted PostgreSQL).

## 📂 Project Structure

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
├── app.py
├── LICENCE
├── README.md
├── README.pt-br.md
└── requirements.txt
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
