import os
import yfinance as yf
import pandas as pd

# Project root (where this script is located)
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Excel file inside the data folder
EXCEL_FILE = os.path.join(BASE_DIR, "data", "ind_nifty500list.xlsx")

SHEET_NAME = "Sheet1"       # Sheet name in Excel file
COLUMN_NAME = "FinalSymbol" # Column name in Excel file containing stock symbols

# Timeframes and intervals
TIMEFRAMES = {
    "day": "1d"
}

# Ensure directories exist
def ensure_directories():
    for tf in TIMEFRAMES:
        dir_path = os.path.join(BASE_DIR, "data", tf)
        os.makedirs(dir_path, exist_ok=True)
        print(f"📂 Directory ready: {dir_path}")

# Read stock list from Excel
def get_stock_list_from_excel():
    if not os.path.exists(EXCEL_FILE):
        raise FileNotFoundError(f"Excel file not found: {EXCEL_FILE}")
    
    df = pd.read_excel(EXCEL_FILE, sheet_name=SHEET_NAME)
    
    if COLUMN_NAME not in df.columns:
        raise ValueError(f"Column '{COLUMN_NAME}' not found in Excel sheet '{SHEET_NAME}'")
    
    stock_list = df[COLUMN_NAME].dropna().unique().tolist()
    stock_list = [str(s).upper() for s in stock_list]
    
    print(f"📊 Loaded {len(stock_list)} stocks from Excel")
    return stock_list

# Fetch and save OHLC data
def fetch_ohlc(stock_list, period="10y"):
    ensure_directories()

    for stock in stock_list:
        print(f"🔹 Fetching data for: {stock}")
        try:
            ticker = yf.Ticker(stock)
            for tf, interval in TIMEFRAMES.items():
                df = ticker.history(period=period, interval=interval)
                
                if df.empty:
                    print(f"⚠️ No data returned for {stock} ({tf})")
                    continue

                df.reset_index(inplace=True)

                # Save CSV in data/<timeframe>/ folder
                file_path = os.path.join(BASE_DIR, "data", tf, f"{stock}.csv")
                df.to_csv(file_path, index=False)
                print(f"✅ Saved {tf} data to {file_path}")
                print(f"   Preview:\n{df.head(3)}\n")  # Show first 3 rows

        except Exception as e:
            print(f"❌ Error fetching {tf} data for {stock}: {e}")

# Run
if __name__ == "__main__":
    try:
        stocks = get_stock_list_from_excel()
        fetch_ohlc(stocks)
        print("🎉 All data fetching completed!")
    except Exception as e:
        print(f"❌ Script failed: {e}")
