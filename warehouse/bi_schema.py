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

# CREATE BI SCHEMA + TABLES
with engine.begin() as conn:
    conn.execute(text("""
        CREATE SCHEMA IF NOT EXISTS bi_schema;

        DROP TABLE IF EXISTS bi_schema.fact_annonce CASCADE;
        DROP TABLE IF EXISTS bi_schema.dim_location CASCADE;
        DROP TABLE IF EXISTS bi_schema.dim_property CASCADE;
        DROP TABLE IF EXISTS bi_schema.dim_price CASCADE;

        CREATE TABLE bi_schema.dim_location(
            location_id SERIAL PRIMARY KEY,
            ville VARCHAR(255),
            quartier VARCHAR(255)
        );

        CREATE TABLE bi_schema.dim_property(
            property_id SERIAL PRIMARY KEY,
            title TEXT,
            link TEXT
        );

        CREATE TABLE bi_schema.dim_price(
            price_id SERIAL PRIMARY KEY,
            price_category VARCHAR(100)
        );

        CREATE TABLE bi_schema.fact_annonce(
            annonce_id SERIAL PRIMARY KEY,
            price BIGINT,
            price_m INT,
            surface INT,
            rooms INT,
            baths INT,
            location_id INT,
            property_id INT,
            price_id INT,
            FOREIGN KEY (location_id) REFERENCES bi_schema.dim_location(location_id),
            FOREIGN KEY (property_id) REFERENCES bi_schema.dim_property(property_id),
            FOREIGN KEY (price_id) REFERENCES bi_schema.dim_price(price_id)
        );
    """))

logging.info("Tables created")

# LOAD CLEAN DATA
df = pd.read_csv(r"C:\Users\user\Desktop\Scraping, Data Cleaning & Data Warehouse Modeling\data\avito_data_clean.csv")
logging.info(f"Loaded clean data: {len(df)} rows")

# PREPARE DIMENSIONS
df_dim_location = df[["ville", "quartier"]].drop_duplicates()
df_dim_property = df[["title", "link"]].drop_duplicates()
df_dim_price = df[["price_category"]].drop_duplicates()

# LOAD DIMENSIONS
df_dim_location.to_sql("dim_location", engine, schema="bi_schema", if_exists="append", index=False)
df_dim_property.to_sql("dim_property", engine, schema="bi_schema", if_exists="append", index=False)
df_dim_price.to_sql("dim_price", engine, schema="bi_schema", if_exists="append", index=False)

logging.info("Dimensions loaded")

# RELOAD DIMENSIONS WITH IDs
df_dim_location = pd.read_sql("SELECT * FROM bi_schema.dim_location", engine)
df_dim_property = pd.read_sql("SELECT * FROM bi_schema.dim_property", engine)
df_dim_price = pd.read_sql("SELECT * FROM bi_schema.dim_price", engine)

# MERGE KEYS
df = df.merge(df_dim_location, on=["ville", "quartier"], how="left")
df = df.merge(df_dim_property, on=["title", "link"], how="left")
df = df.merge(df_dim_price, on=["price_category"], how="left")

# FACT TABLE
df_fact = df[[
    "price",
    "price_m",
    "surface",
    "rooms",
    "baths",
    "location_id",
    "property_id",
    "price_id"
]]

# LOAD FACT TABLE
df_fact.to_sql(
    "fact_annonce",
    engine,
    schema="bi_schema",
    if_exists="append",
    index=False,
    method="multi",
    chunksize=500
)

logging.info(f"Fact table loaded: {len(df_fact)} rows")