# Task 3 — CRUD & Time-Series Query API

Full CRUD (POST, GET, PUT, DELETE) endpoints for **both MySQL and MongoDB** databases created in Task 2.

## Prerequisites

- MySQL Server running on `localhost:3306` with the `global_economy_db` database loaded (run `python task2/mysql_setup.py` first).
- MongoDB Server running on `localhost:27017` with data loaded (run `python task2/mongodb_setup.py` first).

## Install

```bash
pip install -r task3/requirements.txt
```

## Run API

```bash
uvicorn task3.api_demo:app --reload --port 8000
```

Interactive docs are available at `http://localhost:8000/docs`.

## Endpoints

### Health

- `GET /health`

### SQL (MySQL) — CRUD

| Method   | Endpoint                       | Description                        |
|----------|--------------------------------|------------------------------------|
| `POST`   | `/api/sql/records`             | Create a new economic record       |
| `GET`    | `/api/sql/records?country=...` | Get all records for a country      |
| `GET`    | `/api/sql/records/latest?country=...` | Get latest record           |
| `GET`    | `/api/sql/records/range?country=...&start_date=...&end_date=...` | Records by date range |
| `PUT`    | `/api/sql/records/{record_id}` | Update an existing record          |
| `DELETE` | `/api/sql/records/{record_id}` | Delete a record                    |

### MongoDB — CRUD

| Method   | Endpoint                           | Description                        |
|----------|------------------------------------|------------------------------------|
| `POST`   | `/api/mongodb/records`             | Create a new economic record       |
| `GET`    | `/api/mongodb/records?country=...` | Get all records for a country      |
| `GET`    | `/api/mongodb/records/latest?country=...` | Get latest record           |
| `GET`    | `/api/mongodb/records/range?country=...&start_date=...&end_date=...` | Records by date range |
| `PUT`    | `/api/mongodb/records/{record_id}` | Update an existing record          |
| `DELETE` | `/api/mongodb/records/{record_id}` | Delete a record                    |

## Example Requests

**Create a record (SQL):**
```bash
curl -X POST http://localhost:8000/api/sql/records \
  -H "Content-Type: application/json" \
  -d '{"country_name": "United States", "year": 2023, "gdp": 25000000, "population": 335000000}'
```

**Get latest record (MongoDB):**
```bash
curl "http://localhost:8000/api/mongodb/records/latest?country=United%20States"
```

**Get records by date range (SQL):**
```bash
curl "http://localhost:8000/api/sql/records/range?country=United%20States&start_date=2015-01-01&end_date=2021-12-31"
```

**Update a record (SQL):**
```bash
curl -X PUT http://localhost:8000/api/sql/records/42 \
  -H "Content-Type: application/json" \
  -d '{"gdp": 26000000}'
```

**Delete a record (MongoDB):**
```bash
curl -X DELETE http://localhost:8000/api/mongodb/records/64a1b2c3d4e5f6a7b8c9d0e1
```

All endpoints return JSON. Date values are formatted as `YYYY-MM-DD`.
