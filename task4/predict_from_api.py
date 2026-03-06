import argparse
import json
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
import requests


def fetch_time_series(api_url: str, start_date: str | None, end_date: str | None) -> pd.DataFrame:
    params = {}
    if start_date:
        params["start_date"] = start_date
    if end_date:
        params["end_date"] = end_date

    response = requests.get(api_url, params=params, timeout=30)
    response.raise_for_status()
    payload = response.json()

    if isinstance(payload, dict) and "data" in payload:
        payload = payload["data"]

    records = pd.DataFrame(payload)
    if records.empty:
        raise ValueError("No records returned from API.")
    return records


def preprocess_like_training(df: pd.DataFrame, cfg: dict) -> pd.DataFrame:
    timestamp_col = cfg["timestamp_col"]
    target_col = cfg["target_col"]
    base_features = cfg["base_features"]
    lags = cfg.get("lags", [1, 3, 7])
    moving_average_windows = cfg.get("moving_average_windows", [3, 7])

    if timestamp_col not in df.columns:
        raise ValueError(f"Timestamp column '{timestamp_col}' not found in API payload.")
    if target_col not in df.columns:
        raise ValueError(f"Target column '{target_col}' not found in API payload.")

    df[timestamp_col] = pd.to_datetime(df[timestamp_col], errors="coerce")
    df = df.dropna(subset=[timestamp_col]).sort_values(timestamp_col).reset_index(drop=True)

    numeric_columns = [target_col] + base_features
    for column_name in numeric_columns:
        if column_name in df.columns:
            df[column_name] = pd.to_numeric(df[column_name], errors="coerce")

    existing_numeric_columns = [column_name for column_name in numeric_columns if column_name in df.columns]
    if existing_numeric_columns:
        df[existing_numeric_columns] = df[existing_numeric_columns].ffill().bfill()

    for lag_value in lags:
        df[f"{target_col}_lag_{lag_value}"] = df[target_col].shift(lag_value)

    for window_size in moving_average_windows:
        df[f"{target_col}_ma_{window_size}"] = df[target_col].rolling(window=window_size).mean()

    df["day_of_week"] = df[timestamp_col].dt.dayofweek
    df["month"] = df[timestamp_col].dt.month

    df = df.dropna().reset_index(drop=True)
    if df.empty:
        raise ValueError("Not enough records to build lag and moving-average features.")
    return df


def load_artifacts(artifacts_dir: str):
    artifacts_path = Path(artifacts_dir)
    model = joblib.load(artifacts_path / "model.joblib")

    scaler_path = artifacts_path / "scaler.joblib"
    scaler = joblib.load(scaler_path) if scaler_path.exists() else None

    with open(artifacts_path / "feature_order.json", "r", encoding="utf-8") as feature_file:
        feature_order = json.load(feature_file)["feature_order"]

    with open(artifacts_path / "preprocess_config.json", "r", encoding="utf-8") as config_file:
        preprocess_config = json.load(config_file)

    return model, scaler, feature_order, preprocess_config


def make_prediction(df: pd.DataFrame, model, scaler, feature_order: list[str]):
    missing_features = [feature_name for feature_name in feature_order if feature_name not in df.columns]
    if missing_features:
        raise ValueError(f"Missing required features for prediction: {missing_features}")

    latest_features = df.iloc[[-1]][feature_order].copy()
    feature_values = scaler.transform(latest_features) if scaler is not None else latest_features.values

    prediction = model.predict(feature_values)
    prediction_value = prediction[0].item() if hasattr(prediction[0], "item") else prediction[0]

    confidence = None
    if hasattr(model, "predict_proba"):
        probabilities = model.predict_proba(feature_values)
        confidence = float(np.max(probabilities))

    return prediction_value, confidence


def parse_args():
    parser = argparse.ArgumentParser(description="Task 4 prediction script: API fetch, preprocess, model load, forecast")
    parser.add_argument("--api_url", required=True, help="API endpoint returning time-series records")
    parser.add_argument("--artifacts_dir", default="task4/artifacts", help="Folder containing model and config artifacts")
    parser.add_argument("--start_date", default=None, help="Optional start date (YYYY-MM-DD)")
    parser.add_argument("--end_date", default=None, help="Optional end date (YYYY-MM-DD)")
    return parser.parse_args()


def main():
    args = parse_args()

    model, scaler, feature_order, preprocess_config = load_artifacts(args.artifacts_dir)
    source_df = fetch_time_series(args.api_url, args.start_date, args.end_date)
    prepared_df = preprocess_like_training(source_df, preprocess_config)
    prediction, confidence = make_prediction(prepared_df, model, scaler, feature_order)

    print("=== Task 4 Forecast Result ===")
    print(f"Records fetched: {len(source_df)}")
    print(f"Records used after preprocessing: {len(prepared_df)}")
    print(f"Predicted value/class: {prediction}")
    if confidence is not None:
        print(f"Prediction confidence: {confidence:.4f}")


if __name__ == "__main__":
    main()
