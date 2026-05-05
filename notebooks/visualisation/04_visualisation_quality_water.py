from pyspark.sql.functions import col

# Lecture de la table Gold
df_gold = spark.table("gold_qualite_commune_parametre")

# Aperçu brut
#display(df_gold.limit(20))

# filtrer sur un département
if "departement" in df_gold.columns:
    df_35 = df_gold.filter(col("departement") == "35")
    display(df_35)

# évolution d'un paramètre dans le temps
if {"annee", "code_parametre", "valeur_moyenne"} <= set(df_gold.columns):
    df_param = (
        df_gold
        .filter(col("code_parametre") == "NITRATES")  
        .groupBy("annee")
        .avg("valeur_moyenne")
        .orderBy("annee")
    )
    display(df_param)
