import os
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError, OperationalError
from urllib.parse import quote_plus  # for encoding special characters in password

DB_HOST = "192.168.0.110"
DB_PORT = "5432"
DB_NAME = "stockagentdb"
DB_USER = "dbuser"
DB_PASS = "dbuser@123"

def create_and_populate_db():
    # URL-encode password to handle special characters like '@'
    encoded_pass = quote_plus(DB_PASS)

    # Create Postgres engine with encoded password
    engine_url = f"postgresql+psycopg2://{DB_USER}:{encoded_pass}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    print(f"Connecting to database: {DB_NAME} on host: {DB_HOST}:{DB_PORT} as user: {DB_USER}")

    try:
        engine = create_engine(engine_url)
        
        # Test the connection
        with engine.connect() as conn:
            db_name = conn.execute(text("SELECT current_database();")).scalar()
            print(f"✅ Successfully connected to database: {db_name}")

        # Create table if not exists using transaction
        with engine.begin() as conn:  # engine.begin() ensures automatic commit
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
            print("✅ Table 'stock_data' is ready")

    except OperationalError as oe:
        print(f"❌ Could not connect to the database: {oe}")
        return
    except SQLAlchemyError as e:
        print(f"❌ SQLAlchemy error: {e}")
        return

    # Read CSVs and insert into Postgres
    for filename in os.listdir("data/day"):
        if filename.lower().endswith(".csv"):  # handle .CSV or .csv
            try:
                file_path = os.path.join("data/day", filename)
                df = pd.read_csv(file_path)
                df.columns = df.columns.str.strip()

                # Add Stock column from filename
                stock_name = filename.replace(".csv", "").replace(".CSV", "").strip()
                df["Stock"] = stock_name

                # Fix column naming
                df = df.rename(columns={"Stock Splits": "Stock_Splits"})

                # Write to Postgres using transaction
                with engine.begin() as conn:
                    df.to_sql(
                        "stock_data",
                        conn,
                        if_exists="append",
                        index=False,
                        chunksize=5000,
                        method="multi"
                    )
                print(f"✅ Loaded {filename} ({len(df)} rows) into '{DB_NAME}'")

            except Exception as e:
                print(f"❌ Failed to load {filename}: {e}")

    print("📦 Done populating stock_data (Postgres)")

# Run the function
if __name__ == "__main__":
    create_and_populate_db()
