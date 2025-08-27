import os
import pandas as pd
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus  # <-- for encoding special characters in password

DB_HOST = "192.168.0.110"
DB_PORT = "5432"
DB_NAME = "stockagentdb"
DB_USER = "dbuser"
DB_PASS = "dbuser@123"

def create_and_populate_db():
    # URL-encode password to handle special characters like '@'
    encoded_pass = quote_plus(DB_PASS)

    # Create Postgres engine with encoded password
    engine = create_engine(
        f"postgresql+psycopg2://{DB_USER}:{encoded_pass}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    )

    # Create table if not exists
    with engine.connect() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS stock_data (
                Date TIMESTAMP,
                Open DOUBLE PRECISION,
                High DOUBLE PRECISION,
                Low DOUBLE PRECISION,
                Close DOUBLE PRECISION,
                Volume BIGINT,
                Dividends DOUBLE PRECISION,
                Stock_Splits DOUBLE PRECISION,
                Stock TEXT
            )
        """))
        conn.commit()

    # Read CSVs and insert into Postgres
    for filename in os.listdir("data/day"):
        if filename.endswith(".csv"):
            try:
                file_path = os.path.join("data/day", filename)
                df = pd.read_csv(file_path)
                df.columns = df.columns.str.strip()

                # Add Stock column from filename
                stock_name = filename.replace(".csv", "").replace(".CSV", "").strip()
                df["Stock"] = stock_name

                # Fix column naming
                df = df.rename(columns={"Stock Splits": "Stock_Splits"})

                # Write to Postgres
                df.to_sql(
                    "stock_data",
                    engine,
                    if_exists="append",
                    index=False,
                    chunksize=5000,
                    method="multi"
                )
                print(f"✅ Loaded {filename} ({len(df)} rows)")
            except Exception as e:
                print(f"❌ Failed to load {filename}: {e}")

    print("📦 Done populating stock_data (Postgres)")

# Run the function
if __name__ == "__main__":
    create_and_populate_db()
