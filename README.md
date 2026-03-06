# Building a Pipeline for Time Series Data

A time-series data pipeline using the **Global Economy Indicators (1970–2021)** dataset covering exploratory analysis and database design.

## Dataset

**Global Economy Indicators** — 10,500+ records across 220 countries and 52 years.
Source: [Kaggle](https://www.kaggle.com/) / UN Statistics Division
Target variable: **Gross Domestic Product (GDP)**

## Project Structure

```
├── Building_a_Pipeline_for_Time_Series_Data.ipynb  # Task 1: EDA, preprocessing, model training
├── Global Economy Indicators.csv      # Dataset
├── task2/                             # Task 2: Database design & implementation
│   ├── sql_schema.sql                 #   SQL DDL scripts (3 tables)
│   ├── erd_diagram.png                #   Entity Relationship Diagram
│   ├── mysql_setup.py                 #   MySQL: setup, load data, run queries
│   ├── mongodb_setup.py               #   MongoDB: setup, load data, run queries
│   └── requirements.txt               #   Python dependencies for Task 2
├── task4/                             # Task 4: End-to-end prediction script
│   ├── predict_from_api.py            #   Fetch API data -> preprocess -> load model -> predict
│   ├── requirements.txt               #   Python dependencies for Task 4
│   ├── README.md                      #   Task 4 usage instructions
│   └── artifacts/                     #   Model and preprocessing artifacts
│       ├── preprocess_config.json     #   Preprocessing + feature engineering settings
│       ├── feature_order.json         #   Feature order expected by model
│       ├── model.joblib               #   Trained model (add after training)
│       └── scaler.joblib              #   Optional scaler (if used in training)
└── README.md
```

## Task 2 — Database Design & Implementation

### Prerequisites

- Python 3.10+
- MySQL Server 8.x
- MongoDB Server 6.x+

### Installation

```bash
pip install -r task2/requirements.txt
```

### Running

**MySQL** (make sure MySQL is running on localhost:3306):
```bash
python task2/mysql_setup.py
```

**MongoDB** (make sure MongoDB is running on localhost:27017):
```bash
python task2/mongodb_setup.py
```

Both scripts will:
1. Create the database/collection
2. Load all 10,512 records from the CSV
3. Execute 5 analytical queries and display results

### SQL Schema (3 Normalized Tables)

| Table | Description |
|-------|-------------|
| `countries` | Static country info (id, name, currency) |
| `exchange_rates` | Yearly exchange rates per country (FK → countries) |
| `economic_indicators` | 20+ yearly macro metrics per country (FK → countries) |

See `task2/erd_diagram.png` for the full ERD.

### MongoDB Collection Design

Single collection `economic_records` with embedded subdocuments:
- `exchange_rates` — AMA and IMF rates
- `demographics` — population, GNI per capita
- `economic_indicators` — GDP, exports, imports, consumption, etc.
- `sector_breakdown` — agriculture, manufacturing, construction, etc.

### Queries Performed (5 per database)

| # | Query | MySQL | MongoDB |
|---|-------|-------|---------|
| 1 | Top 10 countries by GDP (2021) | JOIN + ORDER BY | find + sort + limit |
| 2 | US economic indicators by date range (2015–2021) | WHERE BETWEEN | find with $gte/$lte |
| 3 | Average GDP by currency / Records per country | GROUP BY + AVG | $group aggregation |
| 4 | Latest record for a country | ORDER BY year DESC LIMIT 1 | find_one + sort |
| 5 | GDP with exchange rates over decades / Avg GNI by currency | 3-table JOIN | $match + $group |

## Task 4 — Prediction/Forecast Script

The Task 4 pipeline script is in `task4/predict_from_api.py` and performs:

1. Fetching time-series records from an API endpoint
2. Preprocessing with lag and moving-average features (aligned with Task 1 style)
3. Loading trained model artifacts (`model.joblib`, optional `scaler.joblib`)
4. Generating a prediction/forecast

### Setup

```bash
pip install -r task4/requirements.txt
```

### Example Run

```bash
python task4/predict_from_api.py --api_url "http://localhost:8000/api/sql/records/range" --start_date "2015-01-01" --end_date "2021-12-31"
```

See `task4/README.md` for full details.