from pyspark.sql.functions import col
import matplotlib.pyplot as plt

# --- Lecture de la table Gold principale ---
df_gold = spark.table(
    "hd_pipeline_databricks_quality_water.gold.gold_qualite_commune_parametre"
)

display(df_gold.limit(20))

# --- Filtrer sur un département (ex : 35) ---
if "departement" in df_gold.columns:
    df_dep = df_gold.filter(col("departement") == "35")
    display(df_dep)

# --- Évolution d'un paramètre dans le temps (ex : NITRATES) ---
if {"annee", "code_parametre", "valeur_moyenne"} <= set(df_gold.columns):
    df_param = (
        df_gold
        .filter(col("code_parametre") == "NITRATES")
        .groupBy("annee")
        .avg("valeur_moyenne")
        .orderBy("annee")
    )
    display(df_param)

    pdf_param = df_param.toPandas()
    plt.figure(figsize=(10,5))
    plt.plot(pdf_param["annee"], pdf_param["avg(valeur_moyenne)"], marker="o")
    plt.title("Évolution des nitrates dans le temps")
    plt.xlabel("Année")
    plt.ylabel("Valeur moyenne")
    plt.grid()
    plt.show()

# --- Moyenne par commune ---
if {"code_commune", "valeur_moyenne"} <= set(df_gold.columns):
    df_commune = (
        df_gold
        .groupBy("code_commune")
        .avg("valeur_moyenne")
        .orderBy(col("avg(valeur_moyenne)").desc())
    )
    display(df_commune)

# --- Moyenne par paramètre ---
if {"code_parametre", "valeur_moyenne"} <= set(df_gold.columns):
    df_parametre = (
        df_gold
        .groupBy("code_parametre")
        .avg("valeur_moyenne")
        .orderBy("code_parametre")
    )
    display(df_parametre)
