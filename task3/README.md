# Task 3 — Demo API for Time-Series Queries

This API provides endpoints required by the assignment and can be used directly by `task4/predict_from_api.py`.

## Install

```bash
pip install -r task3/requirements.txt
```

## Run API

```bash
uvicorn task3.api_demo:app --reload --port 8000
```

## Endpoints

- `GET /health`
- `GET /api/sql/records?country=United%20States`
- `GET /api/sql/records/latest?country=United%20States`
- `GET /api/sql/records/range?country=United%20States&start_date=2015-01-01&end_date=2021-12-31`

All endpoints return JSON and date values are formatted as `YYYY-MM-DD`.
