# ============================================================
# 05_Visualisation_Quality_Water.py
# Visualisation des données Gold – v4.2.0
# ============================================================

from pyspark.sql.functions import col
import matplotlib.pyplot as plt

# ------------------------------------------------------------
# 1. Charger la table Gold principale
# ------------------------------------------------------------
df_gold = spark.table(
    "hd_pipeline_databricks_quality_water.gold.gold_qualite_commune_parametre"
)

display(df_gold.limit(20))

# ------------------------------------------------------------
# 2. Filtrer sur un département (ex : 35)
# ------------------------------------------------------------
if "departement" in df_gold.columns:
    df_dep = df_gold.filter(col("departement") == "35")
    display(df_dep)

# ------------------------------------------------------------
# 3. Évolution d’un paramètre dans le temps (ex : NITRATES)
# ------------------------------------------------------------
required_cols = {"annee", "code_parametre", "valeur_moyenne"}

if required_cols.issubset(df_gold.columns):
    df_param = (
        df_gold
        .filter(col("code_parametre") == "NITRATES")
        .groupBy("annee")
        .avg("valeur_moyenne")
        .orderBy("annee")
    )

    display(df_param)

    pdf_param = df_param.toPandas()

    plt.figure(figsize=(10, 5))
    plt.plot(pdf_param["annee"], pdf_param["avg(valeur_moyenne)"], marker="o")
    plt.title("Évolution des nitrates dans le temps")
    plt.xlabel("Année")
    plt.ylabel("Valeur moyenne")
    plt.grid()
    plt.show()

# ------------------------------------------------------------
# 4. Moyenne par commune
# ------------------------------------------------------------
if {"code_commune", "valeur_moyenne"}.issubset(df_gold.columns):
    df_commune = (
        df_gold
        .groupBy("code_commune")
        .avg("valeur_moyenne")
        .orderBy(col("avg(valeur_moyenne)").desc())
    )
    display(df_commune)

# ------------------------------------------------------------
# 5. Moyenne par paramètre
# ------------------------------------------------------------
if {"code_parametre", "valeur_moyenne"}.issubset(df_gold.columns):
    df_parametre = (
        df_gold
        .groupBy("code_parametre")
        .avg("valeur_moyenne")
        .orderBy("code_parametre")
    )
    display(df_parametre)

print("🎉 Visualisations terminées.")
