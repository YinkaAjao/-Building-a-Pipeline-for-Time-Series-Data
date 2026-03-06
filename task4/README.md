# Task 4 — Prediction/Forecast Script

This folder contains the end-to-end script required for Task 4:

1. Fetches time-series records from your API
2. Applies the same preprocessing logic style used in Task 1
3. Loads trained model artifacts
4. Produces a prediction/forecast

## Files

- `predict_from_api.py` — Main Task 4 script
- `requirements.txt` — Python dependencies
- `artifacts/preprocess_config.json` — Feature engineering + preprocessing config
- `artifacts/feature_order.json` — Ordered feature names expected by the trained model

## Required Model Artifacts

Place these files in `task4/artifacts/` before running:

- `model.joblib` (required)
- `feature_order.json` (required)
- `preprocess_config.json` (required)
- `scaler.joblib` (optional, if used during training)

## Install

```bash
pip install -r task4/requirements.txt
```

## Run

```bash
python task4/predict_from_api.py --api_url "http://localhost:8000/api/sql/records/range" --start_date "2015-01-01" --end_date "2021-12-31"
```

You can also run without date filters:

```bash
python task4/predict_from_api.py --api_url "http://localhost:8000/api/sql/records"
```

## Notes

- The API should return either:
  - a list of JSON records, or
  - an object with a `data` key containing the list.
- Update `preprocess_config.json` and `feature_order.json` to match the exact columns used in your Task 1 model pipeline.
