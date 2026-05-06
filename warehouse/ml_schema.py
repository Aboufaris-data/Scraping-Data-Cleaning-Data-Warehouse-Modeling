from dotenv import load_dotenv
import os
import pandas as pd
import logging
from sqlalchemy import create_engine, text

# =========================
# LOGGING
# =========================
os.makedirs("logs", exist_ok=True)
logging.basicConfig(
    filename="logs/ml_pipeline.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

load_dotenv()

# =========================
# DB CONNECTION
# =========================
engine = create_engine(
    f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}"
    f"@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
)

file_path = r"C:\Users\user\Desktop\Scraping, Data Cleaning & Data Warehouse Modeling\clean\avito_data_clean.csv"

try:
    # =========================
    # TEST DB CONNECTION
    # =========================
    with engine.connect() as con:
        print("✅ DB Connected")

    # =========================
    # RESET TABLE
    # =========================
    with engine.begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS ml_schema.obt_annonce;"))
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS ml_schema;"))
        conn.execute(text("""
            CREATE TABLE ml_schema.obt_annonce(
                annonce_id SERIAL PRIMARY KEY,
                price INT,
                price_m INT,
                surface INT,
                rooms INT,
                baths INT,
                title TEXT,
                link TEXT,
                ville VARCHAR,
                quartier VARCHAR,
                price_category VARCHAR
            );
        """))

    print("✅ Table ready")

    # =========================
    # LOAD CSV (FIXED ENCODING)
    # =========================
    df = pd.read_csv(
    file_path,
    encoding="utf-8-sig",
    engine="python",
    on_bad_lines="skip"
)

    # =========================
    # SELECT + CLEAN
    # =========================
    cols = [
        "price", "price_m", "surface", "rooms", "baths",
        "title", "link", "ville", "quartier", "price_category"
    ]

    df = df[cols].drop_duplicates()

    # remove invalid values safely
    df = df[(df["price"] > 0) & (df["surface"] > 0)]

    print(f"🧹 Clean: {len(df)} rows")

    # =========================
    # LOAD INTO POSTGRES
    # =========================
    df.to_sql(
        "obt_annonce",
        engine,
        schema="ml_schema",
        if_exists="append",
        index=False,
        method="multi",
        chunksize=500
    )

    print(f"🚀 Done: {len(df)} rows inserted")

except Exception as e:
    print("❌ Error:", e)
    logging.error("Pipeline failed", exc_info=True)