
# Horse Handicapping ML (2023 → Track App)

End‑to‑end pipeline to ingest Equibase 2023 data, engineer handicapping features, train calibrated ML models, and output bet recommendations for a track‑ready app.

## Quick Start

### 1) Environment
Use conda (recommended) or venv.

```bash
# conda
conda env create -f environment.yml
conda activate hhml

# OR venv
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 2) Local config
Create a `.env` from the example:
```bash
cp .env.example .env
# edit values (Postgres URL, data paths, etc.)
```

### 3) Initialize DB
```bash
psql "$POSTGRES_URL" -f scripts/bootstrap_db.sql
```

### 4) Parse a small sample
```bash
python -m hhml.etl.parse_equibase_xml --in data/raw/sample --kind results --limit 10
```

### 5) Format & Lint
```bash
pre-commit install
pre-commit run --all-files
```

### 6) Tests
```bash
pytest -q
```

## Repo Layout
```
src/hhml/               # Python package
  db/                   # DB connectors & helpers
  etl/                  # parsers & loaders for Equibase XML
  features/             # feature engineering
  models/               # training, calibration, evaluation
  bets/                 # bet synthesis & bankroll
  app/                  # Streamlit/Django front-end (MVP)
scripts/                # SQL DDL, utilities
data/                   # (ignored) raw/interim/processed data
notebooks/              # exploratory work (lightweight EDA)
tests/                  # unit tests
```

## License
MIT (see `LICENSE`).
