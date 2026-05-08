# Databricks notebook source
# ============================================================
# GOLD - AGRÉGATIONS & KPIs QUALITÉ DE L'EAU (Version 4)
# À partir des tables Silver :
#   - silver_analyses
#   - silver_stations
#   - silver_parametres
# ============================================================

# ------------------------------------------------------------
# 0. CATALOG + SCHEMA
# ------------------------------------------------------------
spark.sql("USE CATALOG hd_pipeline_databricks_quality_water")
spark.sql("USE SCHEMA gold")


from pyspark.sql.functions import (
    col,
    count,
    avg,
    sum as _sum,
    when,
    lit
)
from utils.helpers import (
    load_config,
    write_gold,
    log
)

# ------------------------------------------------------------
# 1. Config & chargement Silver
# ------------------------------------------------------------
from config.config import config
CONFIG_PATH = "config/config.py"


tables = config["tables"]

df_silver_analyses = spark.table(f"hd_pipeline_databricks_quality_water.silver.{tables['silver_analyses']}")
df_silver_stations = spark.table(f"hd_pipeline_databricks_quality_water.silver.{tables['silver_stations']}")
df_silver_parametres = spark.table(f"hd_pipeline_databricks_quality_water.silver.{tables['silver_parametres']}")

log("Tables Silver chargées ✔️")

# ------------------------------------------------------------
# 2. Préparation : indicateurs de conformité
# ------------------------------------------------------------
# On suppose une colonne 'conclusion_sur_le_parametre' ou équivalent
# conforme / non conforme / conforme avec remarque
col_conclusion = "conclusion_sur_le_parametre"

df = df_silver_analyses

df = df.withColumn(
    "is_conforme",
    when(col(col_conclusion).rlike("(?i)non conforme"), lit(0))
    .when(col(col_conclusion).isNull(), lit(None))
    .otherwise(lit(1))
)

# ------------------------------------------------------------
# 3. Table GOLD - Qualité par commune & paramètre
# ------------------------------------------------------------
gold_commune_parametre = (
    df.groupBy(
        "code_commune",
        "commune",
        "code_parametre",
        "libelle_parametre",
        "annee"
    )
    .agg(
        count("*").alias("nb_mesures"),
        _sum("is_conforme").alias("nb_conformes"),
        ( _sum("is_conforme") / count("*") ).alias("taux_conformite")
    )
)

write_gold(
    gold_commune_parametre,
    config,
    config["tables"]["gold_commune_parametre"],
    partition_col="annee"
)

log("Table GOLD - commune_parametre écrite ✔️")

# ------------------------------------------------------------
# 4. Table GOLD - KPI conformité par département
# ------------------------------------------------------------
gold_kpi_dept = (
    df.groupBy("code_departement", "nom_departement", "annee")
    .agg(
        count("*").alias("nb_mesures"),
        _sum("is_conforme").alias("nb_conformes"),
        ( _sum("is_conforme") / count("*") ).alias("taux_conformite")
    )
)

write_gold(
    gold_kpi_dept,
    config,
    config["tables"]["gold_kpi_conformite_dept"],
    partition_col="annee"
)

log("Table GOLD - kpi_conformite_departement écrite ✔️")

# ------------------------------------------------------------
# 5. Table GOLD - Tendance par paramètre (évolution temporelle)
# ------------------------------------------------------------
gold_tendance_parametre = (
    df.groupBy("code_parametre", "libelle_parametre", "annee")
    .agg(
        count("*").alias("nb_mesures"),
        _sum("is_conforme").alias("nb_conformes"),
        ( _sum("is_conforme") / count("*") ).alias("taux_conformite")
    )
)

write_gold(
    gold_tendance_parametre,
    config,
    config["tables"]["gold_kpi_tendance_parametre"],
    partition_col="annee"
)

log("Table GOLD - kpi_tendance_parametre écrite ✔️")

# ------------------------------------------------------------
# 6. Table GOLD - Top communes (meilleures & pires)
# ------------------------------------------------------------
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

w_commune = Window.partitionBy("annee").orderBy(col("taux_conformite").desc())
w_commune_bad = Window.partitionBy("annee").orderBy(col("taux_conformite").asc())

base_commune = gold_commune_parametre.filter(col("nb_mesures") >= 10)

top_communes = (
    base_commune
    .withColumn("rk_best", row_number().over(w_commune))
    .withColumn("rk_worst", row_number().over(w_commune_bad))
)

gold_top_communes = top_communes.filter(col("rk_best") <= 10)
gold_top10_non_conformes = top_communes.filter(col("rk_worst") <= 10)

write_gold(
    gold_top_communes,
    config,
    config["tables"]["gold_kpi_top_communes"],
    partition_col="annee"
)

write_gold(
    gold_top10_non_conformes,
    config,
    config["tables"]["gold_kpi_top10_non_conformes"],
    partition_col="annee"
)

log("Tables GOLD - top communes écrites ✔️")

# ------------------------------------------------------------
# 7. Table GOLD - Paramètres les plus problématiques
# ------------------------------------------------------------
gold_param_problemes = (
    gold_tendance_parametre
    .filter(col("nb_mesures") >= 50)
    .withColumn("taux_non_conformite", 1 - col("taux_conformite"))
)

from pyspark.sql.functions import dense_rank
w_param = Window.partitionBy("annee").orderBy(col("taux_non_conformite").desc())

gold_param_problemes = gold_param_problemes.withColumn(
    "rk_param_probleme",
    dense_rank().over(w_param)
).filter(col("rk_param_probleme") <= 10)

write_gold(
    gold_param_problemes,
    config,
    config["tables"]["gold_kpi_top10_parametres_problemes"],
    partition_col="annee"
)

log("Table GOLD - top10_parametres_problemes écrite ✔️")

# ------------------------------------------------------------
# 8. Table GOLD - Carte régionale (agrégation géographique simple)
# ------------------------------------------------------------
# Ici on reste au niveau département (ou tu peux mapper à une région si tu as un référentiel)
gold_carte_regionale = gold_kpi_dept  # simplifié : un KPI par département

write_gold(
    gold_carte_regionale,
    config,
    config["tables"]["gold_kpi_carte_regionale"],
    partition_col="annee"
)

log("🎉 Toutes les tables GOLD ont été générées avec succès !")
