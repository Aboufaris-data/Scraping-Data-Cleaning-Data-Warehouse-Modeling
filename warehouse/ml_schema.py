import os
import logging
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# LOGGING SETUP
os.makedirs("logs", exist_ok=True)

logging.basicConfig(
    filename="logs/bi_pipeline.log",
    filemode="a",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logging.info("Starting BI pipeline...")

# LOAD ENV + DB CONNECTION
load_dotenv()

def get_engine():
    try:
        engine = create_engine(
            f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASS')}"
            f"@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
        )
        logging.info("Database connected")
        return engine
    except Exception as e:
        logging.error(f"DB connection error: {e}")
        raise

engine = get_engine()

with engine.begin() as conn:
    conn.execute(text("""
        CREATE SCHEMA IF NOT EXISTS ml_schema;

        CREATE TABLE ml_schema.obt_annonce(
            annonce_id SERIAL PRIMARY KEY,
            title TEXT,
            link TEXT,
            location TEXT,
            price BIGINT,
            price_m INT,
            surface INT,
            rooms INT,
            baths INT,
            ville VARCHAR,
            quartier VARCHAR,
            price_category VARCHAR
        );
    """))

logging.info("Table OBT is created")

# LOAD CLEAN DATA
df = pd.read_csv(r"C:\Users\user\Desktop\Scraping, Data Cleaning & Data Warehouse Modeling\data\avito_data_clean.csv")

# LOAD FACT TABLE
df.to_sql(
    "obt_annonce",
    engine,
    schema="ml_schema",
    if_exists="append",
    index=False,
    method="multi",
    chunksize=500
)

logging.info(f"OBT table loaded: {len(df)} rows")