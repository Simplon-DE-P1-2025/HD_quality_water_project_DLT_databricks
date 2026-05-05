# --- MOCK SPARK FOR CI ENVIRONMENT ---
try:
    spark  # Vérifie si Spark existe (Databricks)
except NameError:
    class MockSpark:
        def read(self):
            raise RuntimeError("Spark is not available in CI environment")

        def sql(self, *args, **kwargs):
            raise RuntimeError("Spark is not available in CI environment")

    spark = MockSpark()
# -------------------------------------

from pyspark.sql.functions import col

# Lecture de la table Gold
df_gold = spark.table("gold_qualite_commune_parametre")

# --- Aperçu général ---
display(df_gold.limit(20))

# --- Filtrer sur un département (ex : 35) ---
if "departement" in df_gold.columns:
    df_dep = df_gold.filter(col("departement") == "35")
    display(df_dep)

# --- Évolution d'un paramètre dans le temps ---
if {"annee", "code_parametre", "valeur_moyenne"} <= set(df_gold.columns):
    df_param = (
        df_gold
        .filter(col("code_parametre") == "NITRATES")
        .groupBy("annee")
        .avg("valeur_moyenne")
        .orderBy("annee")
    )
    display(df_param)

# Visualisation par commune

if {"commune", "valeur_moyenne"} <= set(df_gold.columns):
    df_commune = (
        df_gold
        .groupBy("commune")
        .avg("valeur_moyenne")
        .orderBy(col("avg(valeur_moyenne)").desc())
    )
    display(df_commune)


# Visualisation par parametre

if {"code_parametre", "valeur_moyenne"} <= set(df_gold.columns):
    df_parametre = (
        df_gold
        .groupBy("code_parametre")
        .avg("valeur_moyenne")
        .orderBy("code_parametre")
    )
    display(df_parametre)
