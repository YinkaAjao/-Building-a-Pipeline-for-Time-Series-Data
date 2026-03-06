from pathlib import Path

import pandas as pd
from fastapi import FastAPI, HTTPException, Query


app = FastAPI(title="Time Series Demo API", version="1.0.0")

BASE_DIR = Path(__file__).resolve().parent.parent
CSV_PATH = BASE_DIR / "Global Economy Indicators.csv"


def _load_dataset() -> pd.DataFrame:
    if not CSV_PATH.exists():
        raise FileNotFoundError(f"Dataset not found: {CSV_PATH}")

    df = pd.read_csv(CSV_PATH)
    df.columns = df.columns.str.strip()

    rename_map = {
        "Country": "Country",
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
    available_map = {source: target for source, target in rename_map.items() if source in df.columns}
    df = df.rename(columns=available_map)

    required_columns = [
        "Country",
        "Year",
        "Population",
        "AMA_exchange_rate",
        "IMF_exchange_rate",
        "Per_capita_GNI",
        "Exports",
        "Imports",
        "GNI_USD",
        "GDP",
    ]
    missing_required = [column_name for column_name in required_columns if column_name not in df.columns]
    if missing_required:
        raise ValueError(f"Missing required dataset columns: {missing_required}")

    df["Country"] = df["Country"].astype(str).str.strip()
    df["Year"] = pd.to_numeric(df["Year"], errors="coerce")
    df = df.dropna(subset=["Country", "Year"]).copy()
    df["Year"] = df["Year"].astype(int)
    df["Date"] = pd.to_datetime(df["Year"].astype(str) + "-01-01", errors="coerce")

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
        df[column_name] = pd.to_numeric(df[column_name], errors="coerce")

    return df.sort_values(["Country", "Year"]).reset_index(drop=True)


def _to_records(df: pd.DataFrame):
    api_df = df.copy()
    api_df["Date"] = api_df["Date"].dt.strftime("%Y-%m-%d")
    return api_df.to_dict(orient="records")


@app.get("/health")
def health_check():
    return {"status": "ok"}


@app.get("/api/sql/records")
def get_records(country: str = Query(..., description="Country name, e.g. United States")):
    try:
        df = _load_dataset()
    except Exception as error:
        raise HTTPException(status_code=500, detail=str(error)) from error

    country_df = df[df["Country"].str.lower() == country.strip().lower()].copy()
    if country_df.empty:
        raise HTTPException(status_code=404, detail=f"No records found for country: {country}")

    return {"country": country, "count": int(len(country_df)), "data": _to_records(country_df)}


@app.get("/api/sql/records/latest")
def get_latest_record(country: str = Query(..., description="Country name, e.g. United States")):
    try:
        df = _load_dataset()
    except Exception as error:
        raise HTTPException(status_code=500, detail=str(error)) from error

    country_df = df[df["Country"].str.lower() == country.strip().lower()].copy()
    if country_df.empty:
        raise HTTPException(status_code=404, detail=f"No records found for country: {country}")

    latest = country_df.sort_values("Year", ascending=False).head(1)
    return {"country": country, "data": _to_records(latest)[0]}


@app.get("/api/sql/records/range")
def get_records_by_date_range(
    country: str = Query(..., description="Country name, e.g. United States"),
    start_date: str = Query(..., description="Start date in YYYY-MM-DD"),
    end_date: str = Query(..., description="End date in YYYY-MM-DD"),
):
    try:
        start = pd.to_datetime(start_date)
        end = pd.to_datetime(end_date)
    except Exception as error:
        raise HTTPException(status_code=400, detail="Invalid date format. Use YYYY-MM-DD") from error

    if end < start:
        raise HTTPException(status_code=400, detail="end_date must be greater than or equal to start_date")

    try:
        df = _load_dataset()
    except Exception as error:
        raise HTTPException(status_code=500, detail=str(error)) from error

    country_df = df[df["Country"].str.lower() == country.strip().lower()].copy()
    if country_df.empty:
        raise HTTPException(status_code=404, detail=f"No records found for country: {country}")

    filtered = country_df[(country_df["Date"] >= start) & (country_df["Date"] <= end)].copy()
    if filtered.empty:
        raise HTTPException(status_code=404, detail="No records in the specified date range")

    return {
        "country": country,
        "start_date": start.strftime("%Y-%m-%d"),
        "end_date": end.strftime("%Y-%m-%d"),
        "count": int(len(filtered)),
        "data": _to_records(filtered),
    }
