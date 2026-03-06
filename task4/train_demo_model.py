from pathlib import Path

import joblib
import pandas as pd
from sklearn.linear_model import LinearRegression
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler


BASE_DIR = Path(__file__).resolve().parent.parent
CSV_PATH = BASE_DIR / "Global Economy Indicators.csv"
ARTIFACTS_DIR = BASE_DIR / "task4" / "artifacts"


def main():
    ARTIFACTS_DIR.mkdir(parents=True, exist_ok=True)

    df = pd.read_csv(CSV_PATH)
    df.columns = df.columns.str.strip()

    renamed = df.rename(
        columns={
            "Year": "Year",
            "Population": "Population",
            "AMA exchange rate": "AMA_exchange_rate",
            "IMF based exchange rate": "IMF_exchange_rate",
            "Per capita GNI": "Per_capita_GNI",
            "Exports of goods and services": "Exports",
            "Imports of goods and services": "Imports",
            "Gross National Income(GNI) in USD": "GNI_USD",
            "Gross Domestic Product (GDP)": "GDP",
        }
    )

    renamed["Date"] = pd.to_datetime(renamed["Year"].astype(str) + "-01-01", errors="coerce")

    numeric_columns = [
        "Population",
        "AMA_exchange_rate",
        "IMF_exchange_rate",
        "Per_capita_GNI",
        "Exports",
        "Imports",
        "GNI_USD",
        "GDP",
    ]

    for column_name in numeric_columns:
        renamed[column_name] = pd.to_numeric(renamed[column_name], errors="coerce")

    renamed = renamed.sort_values(["Country", "Date"]).reset_index(drop=True)
    renamed[numeric_columns] = renamed[numeric_columns].ffill().bfill()

    renamed["GDP_lag_1"] = renamed.groupby("Country")["GDP"].shift(1)
    renamed["GDP_lag_3"] = renamed.groupby("Country")["GDP"].shift(3)
    renamed["GDP_lag_7"] = renamed.groupby("Country")["GDP"].shift(7)
    renamed["GDP_ma_3"] = renamed.groupby("Country")["GDP"].transform(lambda series: series.rolling(3).mean())
    renamed["GDP_ma_7"] = renamed.groupby("Country")["GDP"].transform(lambda series: series.rolling(7).mean())
    renamed["day_of_week"] = renamed["Date"].dt.dayofweek
    renamed["month"] = renamed["Date"].dt.month

    feature_order = [
        "Population",
        "AMA_exchange_rate",
        "IMF_exchange_rate",
        "Per_capita_GNI",
        "Exports",
        "Imports",
        "GNI_USD",
        "GDP_lag_1",
        "GDP_lag_3",
        "GDP_lag_7",
        "GDP_ma_3",
        "GDP_ma_7",
        "day_of_week",
        "month",
    ]

    model_df = renamed.dropna(subset=feature_order + ["GDP"]).copy()
    x_values = model_df[feature_order]
    y_values = model_df["GDP"]

    pipeline = Pipeline(
        steps=[
            ("scaler", StandardScaler()),
            ("regressor", LinearRegression()),
        ]
    )
    pipeline.fit(x_values, y_values)

    model = pipeline.named_steps["regressor"]
    scaler = pipeline.named_steps["scaler"]

    joblib.dump(model, ARTIFACTS_DIR / "model.joblib")
    joblib.dump(scaler, ARTIFACTS_DIR / "scaler.joblib")

    print(f"Saved model to: {ARTIFACTS_DIR / 'model.joblib'}")
    print(f"Saved scaler to: {ARTIFACTS_DIR / 'scaler.joblib'}")


if __name__ == "__main__":
    main()
