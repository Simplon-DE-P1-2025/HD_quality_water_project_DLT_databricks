import dlt
import logging
import yaml
from pyspark.sql.functions import col, year
from utils.api import call_hubeau

# --- Chargement du YAML ---
with open("config/config.yaml", "r") as f:
    cfg = yaml.safe_load(f)

ANNEES = cfg["ingestion"]["annees"]
DEPARTEMENT = cfg["ingestion"]["departement"]
API_BASE_URL = cfg["ingestion"]["api_base_url"]

logging.basicConfig(level=logging.INFO)


@dlt.table(
    name="bronze_analyses",
    comment="Table Bronze contenant les données brutes Hub'Eau.",
    table_properties={"quality": "bronze"}
)
def bronze_analyses():

    logging.info(f"Début ingestion Bronze pour département {DEPARTEMENT}")

    all_data = []

    for annee in ANNEES:
        logging.info(f"Appel API pour année {annee}")

        params = {
            "code_departement": DEPARTEMENT,
            "annee": annee
        }

        data = call_hubeau(
            endpoint="analyses",
            params=params,
            base_url=API_BASE_URL
        )

        logging.info(f"{len(data)} lignes récupérées pour {annee}")
        all_data.extend(data)

    if not all_data:
        logging.warning("Aucune donnée récupérée")
        return spark.createDataFrame([], schema="code_departement string")

    df = spark.createDataFrame(all_data)

    # Ajout de la colonne année
    if "date_prelevement" in df.columns:
        df = df.withColumn("annee", year(col("date_prelevement")))
    else:
        df = df.withColumn("annee", col("annee"))

    logging.info(f"DataFrame Bronze final : {df.count()} lignes")

    return df
