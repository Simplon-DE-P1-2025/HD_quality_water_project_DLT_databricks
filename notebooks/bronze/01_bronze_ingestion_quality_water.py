import dlt
from pyspark.sql.functions import col, year
from utils.api import call_hubeau
import yaml

# --- Chargement du fichier YAML ---
with open("config/config.yaml", "r") as f:
    cfg = yaml.safe_load(f)

# Ingestion
ANNEES = cfg["ingestion"]["annees"]
DEPARTEMENT = cfg["ingestion"]["departement"]
API_BASE_URL = cfg["ingestion"]["api_base_url"]

# Storage
STORAGE_MODE = cfg["storage"]["mode"]
BRONZE_PATH = cfg["storage"][STORAGE_MODE]["bronze_path"]


@dlt.table(
    name="bronze_analyses",
    comment="Table contenant les données brutes des analyses de qualité de l'eau (Hub'Eau).",
    path=BRONZE_PATH
)
def bronze_analyses():
    """
    Ingestion des analyses depuis l'API Hub'Eau pour plusieurs années
    et un département donné.
    """
    all_data = []

    for annee in ANNEES:
        params = {
            "code_departement": DEPARTEMENT,
            "date_debut": f"{annee}-01-01",
            "date_fin": f"{annee}-12-31",
            "size": 5000
        }
        data = call_hubeau("analyses", params, base_url=API_BASE_URL)
        all_data.extend(data)

    if not all_data:
        return spark.createDataFrame([], schema="code_departement string")

    df = spark.createDataFrame(all_data)

    if "date_prelevement" in df.columns:
        df = df.withColumn("annee", year(col("date_prelevement")))
    else:
        df = df.withColumn("annee", col("annee"))

    return df
