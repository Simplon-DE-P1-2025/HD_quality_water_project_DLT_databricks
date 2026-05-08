# ============================================================
# 04_Quality_Checks.py
# Contrôles de qualité avec Great Expectations — v4.1.0
# ============================================================

# ------------------------------------------------------------
# 0. Contexte Unity Catalog
# ------------------------------------------------------------
spark.sql("USE CATALOG hd_pipeline_databricks_quality_water")
spark.sql("USE SCHEMA gold")

import great_expectations as ge
from pyspark.sql import functions as F
from utils.helpers import log
from config.config import config

tables = config["tables"]

# ------------------------------------------------------------
# 1. Helpers pour créer un GE DataFrame
# ------------------------------------------------------------
def ge_from_table(full_table_name: str):
    log(f"🔎 Chargement de la table : {full_table_name}")
    df_spark = spark.table(full_table_name)
    df_ge = ge.dataset.SparkDFDataset(df_spark)
    return df_ge

def log_result(label: str, result):
    status = "✅" if result.success else "❌"
    log(f"{status} {label} — success={result.success}")

# ------------------------------------------------------------
# 2. Contrôles Bronze
# ------------------------------------------------------------
log("=== Contrôles Bronze ===")

bronze_catalog = "hd_pipeline_databricks_quality_water.bronze"

df_bronze_analyses = ge_from_table(f"{bronze_catalog}.{tables['bronze_analyses']}")
df_bronze_stations = ge_from_table(f"{bronze_catalog}.{tables['bronze_stations']}")
df_bronze_parametres = ge_from_table(f"{bronze_catalog}.{tables['bronze_parametres']}")

# Bronze analyses : colonnes essentielles non nulles
res = df_bronze_analyses.expect_column_values_to_not_be_null("code_commune")
log_result("Bronze analyses — code_commune non null", res)

res = df_bronze_analyses.expect_column_values_to_not_be_null("code_parametre")
log_result("Bronze analyses — code_parametre non null", res)

# Bronze stations : code_station présent
if "code_station" in df_bronze_stations.columns:
    res = df_bronze_stations.expect_column_values_to_not_be_null("code_station")
    log_result("Bronze stations — code_station non null", res)

# Bronze parametres : code_parametre unique
if "code_parametre" in df_bronze_parametres.columns:
    res = df_bronze_parametres.expect_column_values_to_be_unique("code_parametre")
    log_result("Bronze parametres — code_parametre unique", res)

# ------------------------------------------------------------
# 3. Contrôles Silver
# ------------------------------------------------------------
log("=== Contrôles Silver ===")

silver_catalog = "hd_pipeline_databricks_quality_water.silver"

df_silver_analyses = ge_from_table(f"{silver_catalog}.{tables['silver_analyses']}")
df_silver_stations = ge_from_table(f"{silver_catalog}.{tables['silver_stations']}")
df_silver_parametres = ge_from_table(f"{silver_catalog}.{tables['silver_parametres']}")

# Silver analyses : types numériques cohérents
for col_name in ["resultat", "limite_qualite", "reference_qualite"]:
    if col_name in df_silver_analyses.columns:
        res = df_silver_analyses.expect_column_values_to_be_of_type(col_name, "DoubleType")
        log_result(f"Silver analyses — {col_name} type double", res)

# Silver analyses : année dans une plage raisonnable
if "annee" in df_silver_analyses.columns:
    res = df_silver_analyses.expect_column_values_to_be_between(
        "annee", min_value=2000, max_value=2030, mostly=0.99
    )
    log_result("Silver analyses — annee entre 2000 et 2030", res)

# Silver stations : pas de doublons sur code_station
if "code_station" in df_silver_stations.columns:
    res = df_silver_stations.expect_column_values_to_be_unique("code_station")
    log_result("Silver stations — code_station unique", res)

# ------------------------------------------------------------
# 4. Contrôles Gold
# ------------------------------------------------------------
log("=== Contrôles Gold ===")

gold_catalog = "hd_pipeline_databricks_quality_water.gold"
gold_main = f"{gold_catalog}.{tables['gold_commune_parametre']}"

df_gold = ge_from_table(gold_main)

# Gold : pas de valeurs négatives sur valeur_moyenne
if "valeur_moyenne" in df_gold.columns:
    res = df_gold.expect_column_values_to_be_greater_than_or_equal_to("valeur_moyenne", 0)
    log_result("Gold — valeur_moyenne >= 0", res)

# Gold : pH dans une plage réaliste si présent
if "code_parametre" in df_gold.columns and "valeur_moyenne" in df_gold.columns:
    df_ph = df_gold.spark_df.filter(F.col("code_parametre") == "PH")
    if df_ph.count() > 0:
        df_ph_ge = ge.dataset.SparkDFDataset(df_ph)
        res = df_ph_ge.expect_column_values_to_be_between(
            "valeur_moyenne", min_value=4.0, max_value=10.0, mostly=0.99
        )
        log_result("Gold — pH entre 4 et 10", res)

log("🎉 Contrôles de qualité terminés (Great Expectations v4.1.0)")
