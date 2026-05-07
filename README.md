# 🏠 Avito Real Estate — Scraping, Data Cleaning & Data Warehouse Modeling

A complete end-to-end data engineering pipeline built on Moroccan real estate listings from [Avito.ma](https://www.avito.ma). The project covers web scraping, data cleaning, and dual data warehouse modeling (BI-oriented Star Schema + ML-oriented OBT).

---

## 📌 Project Overview

| Step | Description |
|---|---|
| **Scraping** | Selenium-based scraper that collects apartment listings from Avito.ma |
| **Data Cleaning** | Jupyter Notebook pipeline that normalizes, enriches, and categorizes raw data |
| **BI Warehouse** | Star Schema (Fact + Dimensions) loaded into PostgreSQL for analytics |
| **ML Warehouse** | One Big Table (OBT) loaded into PostgreSQL, ready for machine learning |

---

## 🗂️ Project Structure

```
├── scrap/
│   └── scraping.py          # Selenium scraper (Avito.ma apartments)
├── nettoyage/
│   └── clean.ipynb          # Data cleaning & feature engineering notebook
├── warehouse/
│   ├── bi_schema.py         # Star Schema: Fact + Dimension tables (BI use case)
│   └── ml_schema.py         # One Big Table schema (ML use case)
├── data/
│   ├── avito_data_raw.csv   # Raw scraped data
│   └── avito_data_clean.csv # Cleaned & enriched data
├── logs/
│   ├── bi_pipeline.log      # BI pipeline execution logs
│   └── ml_pipeline.log      # ML pipeline execution logs
├── .env                     # Database credentials (not committed)
├── .gitignore
└── requirements.txt
```

---

## ⚙️ Pipeline Details

### 1. Web Scraping — `scrap/scraping.py`

Uses **Selenium** with ChromeDriver to scrape apartment listings from Avito.ma across 10 pages.

**Extracted fields:**
- `title` — listing title
- `price` — listing price (MAD)
- `location` — city and neighborhood
- `surface` — area in m²
- `rooms` — number of bedrooms
- `baths` — number of bathrooms
- `link` — URL of the listing

---

### 2. Data Cleaning — `nettoyage/clean.ipynb`

A step-by-step Pandas notebook that transforms raw data into a clean, analysis-ready dataset.

**Cleaning steps:**
- Drop nulls and duplicates
- Extract numeric values from `surface`, `rooms`, and `baths` columns
- Split `location` into `ville` (city) and `quartier` (neighborhood)
- Normalize text casing and remove non-ASCII characters from titles
- Compute `price_m` (price per m²)
- Add `price_category` feature: `Low` (≤ 500K MAD), `Medium`, or `High` (≥ 5M MAD)

---

### 3. BI Warehouse — `warehouse/bi_schema.py`

Implements a **Star Schema** in PostgreSQL under the `bi_schema` namespace, suited for OLAP / BI tools (Power BI, Metabase, etc.).

```
fact_annonce
├── price, price_m, surface, rooms, baths
├── location_id  → dim_location (ville, quartier)
├── property_id  → dim_property (title, link)
└── price_id     → dim_price (price_category)
```

---

### 4. ML Warehouse — `warehouse/ml_schema.py`

Implements a flat **One Big Table (OBT)** in PostgreSQL under the `ml_schema` namespace, optimized for machine learning workflows where denormalized data is preferred.

```
ml_schema.obt_annonce
└── title, link, location, price, price_m, surface, rooms, baths,
    ville, quartier, price_category
```

---

## 🚀 Getting Started

### Prerequisites

- Python 3.8+
- Google Chrome + ChromeDriver (matching versions)
- PostgreSQL database

### Installation

```bash
git clone https://github.com/your-username/your-repo-name.git
cd your-repo-name
pip install -r requirements.txt
```

### Configuration

Create a `.env` file at the root of the project:

```env
DB_USER=your_user
DB_PASS=your_password
DB_HOST=localhost
DB_PORT=5432
DB_NAME=your_database
```

### Running the Pipeline

```bash
# Step 1 — Scrape listings
python scrap/scraping.py

# Step 2 — Clean data (run the notebook)
jupyter notebook nettoyage/clean.ipynb

# Step 3a — Load BI Star Schema
python warehouse/bi_schema.py

# Step 3b — Load ML OBT
python warehouse/ml_schema.py
```

---

## 🛠️ Tech Stack

| Tool | Purpose |
|---|---|
| Python | Core language |
| Selenium | Web scraping |
| Pandas | Data cleaning & transformation |
| SQLAlchemy | Database ORM |
| PostgreSQL | Data warehouse |
| Jupyter Notebook | Interactive data cleaning |
| python-dotenv | Environment variable management |

---

## 📊 Data Source

Listings scraped from **[Avito.ma](https://www.avito.ma)** — Morocco's leading classifieds platform — filtering for apartments for sale with at least 1 bedroom, 1 bathroom, and a listed price.

---

## 📄 License

This project is for educational purposes. Please refer to Avito.ma's terms of service before any production use of scraped data.