# Commodity Supply Chain Pipeline

An end-to-end data engineering pipeline that ingests live commodity and 
shipping data, applies Medallion Architecture transformations, and surfaces 
a supply chain stress signal dashboard showing 2-3 week leading indicators 
of price volatility.

---

## The Business Problem

Global supply chain stress doesn't appear overnight — the signals exist 
in public data days or weeks before prices move. When crude oil inventories 
drop while shipping costs rise simultaneously, that's an early warning of 
a supply squeeze. This pipeline captures and quantifies that signal daily, 
automatically.

---

## Architecture

EIA API (Oil Inventory)  ──┐
├──► S3 Bronze ──► dbt Silver ──► DuckDB Gold ──► Streamlit Dashboard
Baltic Dry Index (BDI)   ──┤                                      │
│                                       ▼
UN Comtrade (India)      ──┘                              Supply Chain Stress Score

> Architecture diagram (visual) coming soon.

**Orchestration:** Apache Airflow DAG runs daily at 06:00 UTC

---

## Data Sources

| Source | What it provides | Update frequency |
|--------|-----------------|-----------------|
| [EIA API](https://www.eia.gov/opendata/) | US crude oil inventory levels | Daily |
| Baltic Dry Index | Global shipping cost proxy | Daily |
| [UN Comtrade](https://comtradeplus.un.org/) | India petroleum import volumes | Monthly |

---

## Medallion Architecture

- **Bronze** — Raw JSON responses stored as-is in AWS S3. No transformation. Full audit trail.
- **Silver** — Cleaned, typed, validated data in Parquet format. Nulls removed, columns standardized.
- **Gold** — Aggregated analytical layer. Rolling averages, correlations, and the composite stress score.

---

## The Stress Signal

stress_score = (BDI_7day_change%) - (EIA_inventory_7day_change%)

A rising stress score indicates shipping demand is increasing while oil 
buffers are shrinking — historically a leading indicator of supply chain 
pressure 2-3 weeks ahead.

---

## Tech Stack

| Layer | Tool |
|-------|------|
| Ingestion | Python, `requests` |
| Storage | AWS S3 |
| Transformation | dbt + DuckDB |
| Orchestration | Apache Airflow |
| Dashboard | Streamlit |

---

## Project Status

- [x] Repository structure initialized
- [ ] Layer 1: EIA ingestion script
- [ ] Layer 1: BDI ingestion script  
- [ ] Layer 2: S3 storage with Hive partitioning
- [ ] Layer 3: dbt Silver models
- [ ] Layer 3: dbt Gold models + stress score
- [ ] Layer 4: Airflow DAG
- [ ] Layer 5: Streamlit dashboard

---

## How to Run Locally

```bash
# Clone the repo
git clone https://github.com/jay-kumar-dubey/Commodity-Supply-Chain-Pipeline.git
cd Commodity-Supply-Chain-Pipeline

# Install dependencies
pip install -r requirements.txt

# Set environment variables
cp .env.example .env
# Edit .env with your actual API keys

# Run EIA ingestion
python ingestion/fetch_eia.py
```

---

## Author

Jay Kumar Dubey — MCA, VIT Bhopal  
[LinkedIn](https://linkedin.com/in/jay-kumar-dubey-137b2823a) | [GitHub](https://github.com/jay-kumar-dubey)
