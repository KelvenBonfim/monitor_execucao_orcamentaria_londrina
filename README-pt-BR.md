# Monitor de Execução Orçamentária de Londrina

Este projeto é um **Sistema de Monitoramento da Execução Orçamentária** para Londrina, permitindo a análise da execução de despesas e receitas municipais ao longo de vários anos.  
Ele suporta **dois modos de dados**: baseado em CSV (KPIs locais) e modo Banco de Dados (PostgreSQL hospedado no Neon).

## 📂 Estrutura do Projeto

```
monitor_execucao_orcamentaria_londrina/
├── data/                           # Armazenamento local de dados
│   └── kpis/                       # KPIs gerados para o modo CSV (por ano)
│       ├── 2018/ ... 2025/         # Uma pasta por ano, contendo:
│       │   ├── data_coverage_report.json
│       │   ├── execucao_global_anual.{csv,json}           # Execução orçamentária anual global
│       │   ├── execucao_por_entidade_anual.{csv,json}     # Execução por entidade
│       │   ├── receita_prevista_arrecadada_anual.{csv,json} # Receita prevista vs arrecadada
│       │   ├── superavit_deficit_anual.{csv,json}         # Resumo de superávit/déficit
│       │   ├── validations_fatos_vs_staging.{csv,json}   # Validações entre tabela de fatos e staging
│       │   └── (outras validações, se geradas)
├── logs/                           # Arquivos de log de execução
│   ├── collect_project_snapshot.log
│   └── load_*.log
├── londrina_dir/                   # Dump em formato de diretório do PostgreSQL (pg_dump -F d)
│   ├── *.dat.gz
│   └── toc.dat
├── outputs/                        # Saídas processadas
│   ├── quality/                    # Resultados da análise de qualidade de dados
│   │   ├── R1_inequalities.csv
│   │   ├── R4_reconcile_fatos_vs_staging.csv
│   │   ├── R6_yoy_anomalias.csv
│   │   └── SUMMARY.csv
│   ├── quality_checks/             # Saídas de verificações automáticas de qualidade
│   │   ├── R1_inequalities.csv
│   │   ├── R4_reconcile_fatos_vs_staging.csv
│   │   └── R6_yoy_anomalias.csv
│   └── reconcile_raw_vs_portal/    # Reconciliação entre dados brutos e do portal
│       └── raw_snapshots/          # Snapshots anuais brutos para reconciliação
│           ├── 2018/ ... 2025/     # Uma pasta por ano, contendo:
│           │   ├── equiplano_empenhadas_anoYYYY.csv
│           │   ├── equiplano_liquidadas_anoYYYY.csv
│           │   └── equiplano_pagas_anoYYYY.csv
├── raw/                            # Conjuntos de dados brutos coletados
│   ├── empenhadas/                 # Empenhos por ano (Equiplano)
│   ├── liquidadas/                 # Liquidações por ano (Equiplano)
│   ├── pagas/                      # Pagamentos por ano (Equiplano)
│   └── receitas_raw/               # Dados brutos de receita do Anexo 10
│       ├── _html_debug/            # Arquivos de debug opcionais
│       └── *.pdf                   # PDFs originais do Anexo 10
├── scripts/                        # Scripts de automação e ETL
│   ├── 01_fetch_equiplano_ano.py       # Baixa CSVs anuais de despesas do Equiplano
│   ├── 02_fetch_receita_prev_arrec.py  # Baixa dados de receita prevista e arrecadada
│   ├── 03_anexo10_pdf_to_csv.py        # Converte o PDF do Anexo 10 para CSV
│   ├── 04_load_csv_to_postgres.py      # Carrega arquivos CSV no PostgreSQL
│   ├── 05_build_models.py              # Constrói tabelas de staging e fatos no PostgreSQL
│   ├── 06_quality_checks.py            # Executa verificações de qualidade e exporta relatórios
│   ├── 07_backfill_historico.py        # Preenche dados históricos no banco de dados
│   ├── 08_reconcile_raw_vs_portal.py   # Faz reconciliação estrita com o portal
│   └── 09_export_kpis.py               # Gera arquivos de KPIs para o modo CSV
├── app.py                          # Aplicação Streamlit para visualização do dashboard
├── LICENCE                         # Arquivo de licença (MIT)
├── README.md                       # Documentação principal (Inglês)
├── README.pt-br.md                 # Documentação em Português
└── requirements.txt                # Dependências Python
```

## ⚙️ Instalação

1. **Clonar o repositório**
```bash
git clone https://github.com/yourusername/londrina-budget-monitor.git
cd londrina-budget-monitor
```

2. **Criar e ativar o ambiente virtual**
```bash
python3 -m venv venv
source venv/bin/activate  # Linux/Mac
venv\Scripts\activate   # Windows
```

3. **Instalar dependências**
```bash
pip install -r requirements.txt
```

## 🗄️ Modos de Dados

O projeto pode funcionar em dois modos:

### **1. Modo CSV (Dados Locais)**
- Os KPIs são pré-gerados e armazenados em `data/kpis/<ano>/`.
- Se os KPIs estiverem ausentes, execute:
```bash
python scripts/09_export_kpis.py
git add data/kpis/
git commit -m "Adicionar KPIs gerados"
```

### **2. Modo Banco de Dados (PostgreSQL no Neon)**
- O dashboard busca os dados de KPI diretamente do banco de dados online.
- Configure seu arquivo `.env` com:
```
DATABASE_URL=postgresql+psycopg2://usuario:senha@host/nomedb
```

## 📊 Visão Geral dos Scripts

| Script | Descrição |
|--------|-----------|
| `01_fetch_equiplano_ano.py` | Baixa CSVs do portal Equiplano para **Empenhadas, Liquidadas, Pagas**. |
| `02_fetch_receita_prev_arrec.py` | Baixa dados de receita prevista e arrecadada. |
| `03_anexo10_pdf_to_csv.py` | Converte o PDF do Anexo 10 para CSV. |
| `04_load_csv_to_postgres.py` | Carrega dados CSV processados em tabelas do PostgreSQL. |
| `05_build_models.py` | Constrói tabelas de fatos no PostgreSQL para schemas de staging e produção. |
| `06_quality_checks.py` | Executa verificações automáticas de qualidade nos dados processados. |
| `07_backfill_historico.py` | Preenche dados orçamentários históricos anteriores ao conjunto principal. |
| `08_reconcile_raw_vs_portal.py` | Compara dados RAW armazenados com os atuais do portal da transparência. |
| `09_export_kpis.py` | Gera arquivos CSV anuais de KPI para uso no dashboard. |

## 🚀 Executando o Dashboard (Streamlit)

1. Certifique-se de que os dados estejam disponíveis (Modo CSV) ou que o banco esteja conectado (Modo Banco de Dados).
2. Inicie o Streamlit:
```bash
streamlit run dashboard/app.py
```

## 🌐 Implantação com Banco Neon

1. Crie um banco de dados no **Neon.tech**.
2. Importe as tabelas de staging usando:
```bash
python scripts/05_build_models.py --schema public --staging public --years 2018-2025 --recreate --verbose
```
3. Atualize o `.env` com a string de conexão do Neon.

## 📈 Arquivos de KPI

No modo CSV, os seguintes arquivos de KPI são necessários em `data/kpis/<ano>/`:
- `execucao_por_funcao_anual.csv`
- `execucao_por_orgao_unidade_anual.csv`

Se não estiverem presentes, execute:
```bash
python scripts/09_export_kpis.py
```

## 📝 Notas

- Use o **modo CSV** para testes offline e reprodutibilidade.
- Use o **modo Banco de Dados** para produção com atualizações em tempo real.
- Sempre faça commit dos arquivos de KPI no modo CSV para controle de versão.

---

**Autor:** Kelven de Alcantara Bonfim  
**Licença:** MIT
