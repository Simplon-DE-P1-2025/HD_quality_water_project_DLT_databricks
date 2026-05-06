# --- MOCK SPARK FOR CI ENVIRONMENT ---
try:
    spark  # Vérifie si Spark existe (Databricks)
except NameError:
    class MockDataFrame:
        def __init__(self):
            self.columns = ["annee", "code_parametre", "valeur_moyenne", "departement", "commune"]

        def limit(self, n):
            return self

        def filter(self, *args, **kwargs):
            return self

        def groupBy(self, *args, **kwargs):
            return self

        def avg(self, *args, **kwargs):
            return self

        def orderBy(self, *args, **kwargs):
            return self

    class MockSpark:
        def table(self, name):
            print(f"[MOCK] spark.table('{name}') called")
            return MockDataFrame()

    spark = MockSpark()


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
