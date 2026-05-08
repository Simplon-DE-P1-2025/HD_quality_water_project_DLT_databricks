# ============================================================
# SILVER TRANSFORMATION - API Hub'Eau (version V4)
# Tables Bronze utilisées :
#   - bronze_analyses
#   - bronze_prelevements
#   - bronze_stations
#   - bronze_parametres
# ============================================================

# ------------------------------------------------------------
# 0. Sélection du CATALOG + SCHEMA (Unity Catalog)
# ------------------------------------------------------------
spark.sql("USE CATALOG hd_pipeline_databricks_quality_water")
spark.sql("USE SCHEMA silver")

from pyspark.sql.functions import col, trim, lower, regexp_replace
from utils.helpers import (
    standardize_column_names,
    clean_string_columns,
    enforce_schema,
    write_silver,
    log
)

# ------------------------------------------------------------
# 1. Charger la configuration
# ------------------------------------------------------------
from config.config import config
CONFIG_PATH = "config/config.py"

tables = config["tables"]

log("Chargement des tables Bronze…")

# ------------------------------------------------------------
# 2. Charger les tables Bronze
# ------------------------------------------------------------
df_analyses_bz = spark.table(f"hd_pipeline_databricks_quality_water.bronze.{tables['bronze_analyses']}")
df_prelevements_bz = spark.table("hd_pipeline_databricks_quality_water.bronze.bronze_prelevements")
df_stations_bz = spark.table(f"hd_pipeline_databricks_quality_water.bronze.{tables['bronze_stations']}")
df_parametres_bz = spark.table(f"hd_pipeline_databricks_quality_water.bronze.{tables['bronze_parametres']}")

log("Tables Bronze chargées ✔️")

# ------------------------------------------------------------
# 3. Nettoyage Silver : standardisation colonnes + trim
# ------------------------------------------------------------
def clean_silver(df):
    df = standardize_column_names(df)
    df = clean_string_columns(df)
    df = df.dropDuplicates()
    return df

df_analyses = clean_silver(df_analyses_bz)
df_prelevements = clean_silver(df_prelevements_bz)
df_stations = clean_silver(df_stations_bz)
df_parametres = clean_silver(df_parametres_bz)

log("Nettoyage Silver appliqué ✔️")

# ------------------------------------------------------------
# 4. Correction des types (schéma Silver)
# ------------------------------------------------------------
schema_analyses = {
    "annee": "int",
    "resultat": "double",
    "limite_qualite": "double",
    "reference_qualite": "double"
}

schema_prelevements = {
    "annee": "int"
}

df_analyses = enforce_schema(df_analyses, schema_analyses)
df_prelevements = enforce_schema(df_prelevements, schema_prelevements)

log("Typage Silver appliqué ✔️")

# ------------------------------------------------------------
# 5. Enrichissement Silver : jointures
# ------------------------------------------------------------

# Join analyses + stations (via code_station)
if "code_station" in df_analyses.columns and "code_station" in df_stations.columns:
    df_analyses = df_analyses.join(
        df_stations.select(
            "code_station", "nom_station", "commune",
            "code_commune", "longitude", "latitude"
        ),
        on="code_station",
        how="left"
    )

# Join analyses + parametres (via code_parametre)
if "code_parametre" in df_analyses.columns and "code_parametre" in df_parametres.columns:
    df_analyses = df_analyses.join(
        df_parametres.select("code_parametre", "libelle_parametre", "unite"),
        on="code_parametre",
        how="left"
    )

log("Enrichissement Silver terminé ✔️")

# ------------------------------------------------------------
# 6. Écriture des tables Silver
# ------------------------------------------------------------

write_silver(df_analyses, config, tables["silver_analyses"], partition_col="annee")
write_silver(df_prelevements, config, tables["silver_prelevements"] if "silver_prelevements" in tables else "silver_prelevements")
write_silver(df_stations, config, tables["silver_stations"])
write_silver(df_parametres, config, tables["silver_parametres"])

log("🎉 Transformation Silver terminée avec succès !")

