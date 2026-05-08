# ============================================================
# 01_Ingestion_Qualite_Eau_Bronze.py
# ETL Bronze — Version v4.0.0 (Hub'Eau qualite_eau_potable)
# ============================================================

# Endpoints utilisés (config.py) :
#   - analyses   -> resultats_dis
#   - prelevements -> prelevements
#   - stations  -> prelevements
#   - parametres -> parametres
#   - communes  -> communes_udi
# ============================================================

# ------------------------------------------------------------
# 0. Sélection du CATALOG + SCHEMA (Unity Catalog)
# ------------------------------------------------------------
spark.sql("USE CATALOG hd_pipeline_databricks_quality_water")
spark.sql("USE SCHEMA bronze")

import time
import requests
from pyspark.sql.functions import col, year

from utils.helpers import (
    list_to_df,
    standardize_column_names,
    clean_string_columns,
    write_bronze,
    log
)

# ------------------------------------------------------------
# 1. Charger la configuration
# ------------------------------------------------------------
from config.config import config
CONFIG_PATH = "config/config.py"   # demandé dans le brief

departement = config["ingestion"]["departement"]
annees = config["ingestion"]["annees"]

BASE_URL = config["ingestion"]["api"]["base_url"]
ENDPOINTS = config["ingestion"]["api"]["endpoints"]

log(f"Département = {departement}, années = {annees}")
log(f"Endpoints utilisés = {ENDPOINTS}")

# ------------------------------------------------------------
# 2. Fonction API robuste avec pagination + retry + pause
# ------------------------------------------------------------
def fetch_all(endpoint, params, retries=5, pause=1.0):
    """
    Appelle l’API Hub’Eau avec :
      - pagination via 'next'
      - retries exponentiels en cas d’erreur réseau
      - pause entre les requêtes pour ne pas saturer l’API
    """
    url = f"{BASE_URL}/{endpoint}"
    results = []

    while url:
        attempt = 0
        while attempt < retries:
            try:
                log(f"🔎 Appel API {endpoint} — url={url} params={params}")
                r = requests.get(url, params=params, timeout=30)
                r.raise_for_status()
                data = r.json()

                batch = data.get("data", [])
                log(f"✅ Réponse API {endpoint} : {len(batch)} lignes")
                results.extend(batch)

                # Pagination
                url = data.get("next", None)
                params = None  # après la première page, on laisse l’API gérer

                # Pause pour ne pas spammer l’API
                time.sleep(pause)
                break

            except Exception as e:
                attempt += 1
                log(f"⚠️ Tentative {attempt}/{retries} échouée sur {endpoint} : {e}")
                time.sleep(pause * attempt)

                if attempt == retries:
                    log(f"❌ Abandon après {retries} tentatives sur {endpoint}")
                    raise e

    log(f"📦 Total récupéré sur {endpoint} : {len(results)} lignes")
    return results

# ------------------------------------------------------------
# 3. Ingestion RESULTATS (resultats_dis)
# ------------------------------------------------------------
log("📡 Récupération des résultats d’analyses (resultats_dis)…")

all_resultats = []
for annee in annees:
    params = {
        "code_departement": departement,
        "annee": annee,
        "size": 2000  # taille raisonnable pour éviter les resets
    }
    log(f"➡️ Année {annee} — params={params}")
    all_resultats.extend(fetch_all(ENDPOINTS["analyses"], params))

df_resultats = list_to_df(spark, all_resultats)
df_resultats = standardize_column_names(df_resultats)
df_resultats = clean_string_columns(df_resultats)

if "date_prelevement" in df_resultats.columns:
    df_resultats = df_resultats.withColumn("annee", year(col("date_prelevement")))

write_bronze(
    df_resultats,
    config,
    config["tables"]["bronze_analyses"],
    partition_col="annee"
)

# ------------------------------------------------------------
# 4. Ingestion PRELEVEMENTS (points de prélèvement)
# ------------------------------------------------------------
log("📡 Récupération des prélèvements (prelevements)…")

all_prelevements = []
for annee in annees:
    params = {
        "code_departement": departement,
        "annee": annee,
        "size": 2000
    }
    log(f"➡️ Année {annee} — params={params}")
    all_prelevements.extend(fetch_all(ENDPOINTS["prelevements"], params))

df_prelevements = list_to_df(spark, all_prelevements)
df_prelevements = standardize_column_names(df_prelevements)
df_prelevements = clean_string_columns(df_prelevements)

write_bronze(
    df_prelevements,
    config,
    "bronze_prelevements"  # nom direct, pas dans config["tables"]
)

# ------------------------------------------------------------
# 5. Ingestion STATIONS (dérivées des prélèvements)
# ------------------------------------------------------------
log("📡 Récupération des stations (via prelevements)…")

# Ici, on peut soit :
#  - réutiliser df_prelevements
#  - soit refaire un appel dédié si besoin de champs spécifiques
# On va partir sur df_prelevements pour limiter la charge API.

df_stations = df_prelevements.select(
    "code_station",
    "nom_station",
    "code_commune",
    "nom_commune",
    "code_departement"
).dropDuplicates()

df_stations = standardize_column_names(df_stations)
df_stations = clean_string_columns(df_stations)

write_bronze(
    df_stations,
    config,
    config["tables"]["bronze_stations"]
)

# ------------------------------------------------------------
# 6. Ingestion PARAMETRES
# ------------------------------------------------------------
log("📡 Récupération des paramètres (parametres)…")

parametres = fetch_all(ENDPOINTS["parametres"], {"size": 2000})
df_parametres = list_to_df(spark, parametres)
df_parametres = standardize_column_names(df_parametres)
df_parametres = clean_string_columns(df_parametres)

write_bronze(
    df_parametres,
    config,
    config["tables"]["bronze_parametres"]
)

log("🎉 Ingestion Bronze terminée avec succès !")
