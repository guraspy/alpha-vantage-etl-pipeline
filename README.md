# Alpha Vantage Stock Data ETL Pipeline

This project is a complete ETL (Extract, Transform, Load) pipeline built in Python. 
It fetches daily stock market data for specified companies (AAPL, GOOG, MSFT) from the Alpha Vantage API, 
transforms the data using Pandas, and loads it into a local SQLite database.

---

### 1. Prerequisites
* **Python 3.8+**
* **An Alpha Vantage API Key** (Sign up on Alpha Vantage and get a free API key)

### 2. Setup
Clone the repository and install the required packages.

```bash
git clone https://github.com/guraspy/alpha-vantage-etl-pipeline
cd alpha-vantage-etl-pipeline
```

### 3. Create and activate a virtual environment
```bash
# For macOS/Linux
python3 -m venv venv
source venv/bin/activate

# For Windows
python -m venv venv
.\venv\Scripts\activate
```
### 4. Install dependencies:
```bash
pip install -r requirements.txt
```

### 5. Set API Key
Set your API key as an environment variable.
```bash
On macOS/Linux:
export ALPHA_VANTAGE_API_KEY="YOUR_API_KEY_HERE"

On Windows:
set ALPHA_VANTAGE_API_KEY="YOUR_API_KEY_HERE"
```
### 6. Run the Pipeline
Run script once:
```bash
python stock_etl.py
```
Run scheduler:
```bash
python scheduler.py
```
