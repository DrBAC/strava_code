<!-- Image aligned to the right with max width -->
<p align="right">
  <img src="assets/DataAnalysis_of_Sports.png" alt="Data Analysis of Sports" style="max-width: 300px; float: right; margin-left: 20px;">
</p>

<h1 style="color: teal;">Data Analysis of Sports</h1>

Welcome to the **Data Analysis of Sports** repository! 🏃‍♂️🚴‍♀️🚶‍♂️

This project explores and visualizes data from various sports activities such as cycling, running, and walking. It aims to uncover trends, performance insights, and actionable metrics using Python and modern data analysis tools.


## Overall Plan

- __Explore & munge data from extracted datasource__
- __Build a db, build the ETL, and insert data into local database__
- __Build Dash App to allow interrogation of training data, leveraging local db connection__
- __Cross correlate with Apple Health data and Weather data__
- __Explore wider data set to try and understand training e.g.:__
   - what does good performance look like (for me)?
   - do I perform better on warmer days?
   - do I perform better after significant rests?
   - can I automatically classify some training - what features enable this?

_I will be removing any personally identifiable information in here before anything is shared, but I will share the code I develop/use to do that_


### Features

- 📊 Interactive data visualizations
- 📍 GPS-based route mapping
- 📈 Performance trend tracking over time
- 🧠 Smart data filtering and grouping

### Tech Stack

- Python (Pandas, NumPy, Matplotlib, Plotly)
- Jupyter Notebooks
- OpenStreetMap / GPX route data
- GitHub Actions for workflow automation

## Getting Started

1. Clone the repository
   ```bash
   git clone https://github.com/DrBAC/strava_code.git
   cd strava_code
   ```
2. Create and activate a virtual environment
   ```bash
   python3 -m venv .venv
   source .venv/bin/activate
   ```
3. Install dependencies
   ```bash
   pip install -r requirements.txt
   ```
4. Place your Strava data export folder in the project root (the folder should be named `STRAVA+export_XXXXXXX`)

5. Run ingestion (DuckDB requires no further setup):
   ```bash
   python -m ingestion.run            # loads activities, routes, bikes
   python -m ingestion.run --streams  # also parses GPS/HR/power time-series (slow)
   ```

6. Open the exploration notebook:
   ```bash
   jupyter notebook exploration.ipynb
   ```
   Select the **Python 3 (.venv)** kernel and set `DB_BACKEND = "duckdb"` in the first cell.

---

## PostgreSQL Setup (Mac)

PostgreSQL is an optional alternative to DuckDB — useful if you want to connect Tableau or other BI tools that don't support DuckDB natively.

### 1. Install PostgreSQL via Homebrew

```bash
brew install postgresql@14
brew services start postgresql@14
```

Verify it's running:
```bash
psql --version
psql postgres   # should open a SQL prompt; type \q to exit
```

### 2. Create the database

```bash
createdb strava
```

No password is needed for a local Homebrew installation — your macOS username is automatically a trusted superuser.

### 3. Configure the .env file

Copy the example file and edit it:
```bash
cp .env.example .env
```

For a standard local Mac install, the only line you need in `.env` is:
```
PG_DSN=postgresql://YOUR_MAC_USERNAME@localhost:5432/strava
```

Replace `YOUR_MAC_USERNAME` with the output of `whoami` in your terminal. No password field is required.

### 4. Run ingestion to PostgreSQL

```bash
python -m ingestion.run --backend postgres
python -m ingestion.run --backend postgres --streams
```

### 5. Connect Tableau

In Tableau Desktop: **Connect → To a Server → PostgreSQL**

| Field    | Value       |
|----------|-------------|
| Server   | `localhost` |
| Port     | `5432`      |
| Database | `strava`    |
| Username | your macOS username |
| Password | *(leave blank)* |

> If Tableau reports a missing driver, go to **Help → Download Drivers** and install the PostgreSQL driver.

---

## License

This project is licensed under the MIT License.

---

Feel free to use as a base for your own work
