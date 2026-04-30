import pandas as pd
import numpy as np
import logging
import os
from datetime import datetime

# -----------------------------
# CONFIGURATION DU LOGGING (FILE + CONSOLE)
# -----------------------------
log_dir = "../logs"
os.makedirs(log_dir, exist_ok=True)

log_file = os.path.join(
    log_dir,
    f"features_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(log_file, encoding="utf-8"),
        logging.StreamHandler()
    ]
)

logging.info("SCRIPT DÉMARRÉ")

# -----------------------------
# CHARGEMENT DES DONNÉES
# -----------------------------
try:
    df = pd.read_csv("../clean/avito_data_cleaned_final.csv")
    logging.info(f"DONNÉES CHARGÉES: {df.shape}")
except Exception as e:
    logging.error(f"ERREUR CHARGEMENT: {e}")
    raise

# -----------------------------
# ANALYSE DES TITRES
# -----------------------------
logging.info(f"Nombre de titres uniques: {df['title'].nunique()}")

# ==================================================
# 1. PRIX PAR M²
# ==================================================
logging.info("Création de price_per_m2...")
df["price_per_m2"] = df["price"] / df["surface"]

# ==================================================
# 2. CATÉGORIE DE SURFACE
# ==================================================
logging.info("Création de surface_category...")

def surface_category(x):
    if x < 50:
        return "petit"
    elif x < 100:
        return "moyen"
    else:
        return "grand"

df["surface_category"] = df["surface"].apply(surface_category)

# ==================================================
# 3. ESTIMATION DE L’ÂGE
# ==================================================
logging.info("Création de estimated_age...")

def estimate_age(title):
    title = str(title).lower()

    if any(word in title for word in ["neuf","nouveau","neuve","jamais habité","like new"]):
        return 0
    elif any(word in title for word in ["moderne","récent","haut standing","lux","luxe"]):
        return 5
    elif any(word in title for word in ["ancien","vieux"]):
        return 30
    else:
        return 15

df["estimated_age"] = df["title"].apply(estimate_age)

# ==================================================
# 4. VARIABLES GÉOGRAPHIQUES
# ==================================================
logging.info("Nettoyage des variables géographiques...")

df["city"] = df["city"].astype(str).str.lower().str.strip()
df["district"] = df["district"].astype(str).str.lower().str.strip()

logging.info("Création de district_freq...")
df["district_freq"] = df["district"].map(df["district"].value_counts())

# ==================================================
# 5. FEATURES TEXTE
# ==================================================
logging.info("Création des features texte...")

df["is_luxury"] = df["title"].str.contains(
    "lux|luxe|villa|haut standing|premium|golf|vue sur mer",
    case=False,
    na=False
)

df["is_new"] = df["title"].str.contains(
    "neuf|nouveau|neuve|jamais habité|like new|récent|moderne",
    case=False,
    na=False
)

# ==================================================
# VÉRIFICATION DES DONNÉES
# ==================================================
logging.info(f"Valeurs manquantes estimated_age: {df['estimated_age'].isna().sum()}")
logging.info(f"Nombre de biens de luxe: {df['is_luxury'].sum()}")
logging.info(f"Nombre de biens neufs: {df['is_new'].sum()}")

# ==================================================
# SAUVEGARDE
# ==================================================
output_path = os.path.join(log_dir, "avito_data_featured.csv")

try:
    df.to_csv(output_path, index=False)
    logging.info(f"FICHIER ENREGISTRÉ: {output_path}")
except Exception as e:
    logging.error(f"ERREUR SAUVEGARDE: {e}")

# -----------------------------
# FIN
# -----------------------------
print(df.head())
logging.info("SCRIPT TERMINÉ")