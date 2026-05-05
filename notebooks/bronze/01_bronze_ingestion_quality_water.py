import dlt
from pyspark.sql.functions import col, year
from utils.api import call_hubeau

# Paramètres simples (tu pourras les externaliser plus tard si tu veux)
ANNEES = [2021, 2022, 2023]
DEPARTEMENT = "35"


@dlt.table(
    name="bronze_analyses",
    comment="Données brutes des analyses de qualité de l'eau (Hub'Eau)."
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
        data = call_hubeau("analyses", params)
        all_data.extend(data)

    if not all_data:
        return spark.createDataFrame([], schema="code_departement string")

    df = spark.createDataFrame(all_data)
    if "date_prelevement" in df.columns:
        df = df.withColumn("annee", year(col("date_prelevement")))
    else:
        df = df.withColumn("annee", col("annee"))

    return df
