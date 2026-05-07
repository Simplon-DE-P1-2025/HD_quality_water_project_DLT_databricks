import dlt
from pyspark.sql.functions import (
    col, avg, min, max, count, year
)

# --- DIM STATIONS ---
@dlt.table(
    name="gold_dim_stations",
    comment="Dimension des stations.",
    table_properties={"quality": "gold"}
)
def gold_dim_stations():
    df = dlt.read("silver_stations")

    cols = [c for c in df.columns if c in {
        "code_station", "nom_station", "code_commune", "departement",
        "coord_x", "coord_y"
    }]

    return df.select(*cols).dropDuplicates(["code_station"])


# --- DIM PARAMETRES ---
@dlt.table(
    name="gold_dim_parametres",
    comment="Dimension des paramètres.",
    table_properties={"quality": "gold"}
)
def gold_dim_parametres():
    df = dlt.read("silver_parametres")

    cols = [c for c in df.columns if c in {
        "code_parametre", "libelle_parametre", "unite"
    }]

    return df.select(*cols).dropDuplicates(["code_parametre"])


# --- DIM TEMPS ---
@dlt.table(
    name="gold_dim_temps",
    comment="Dimension temps (année).",
    table_properties={"quality": "gold"}
)
def gold_dim_temps():
    df = dlt.read("silver_mesures")

    if "date_prelevement" in df.columns:
        df = df.withColumn("annee", year(col("date_prelevement")))

    return df.select("annee").dropDuplicates().orderBy("annee")


# --- TABLE DE FAITS / QUALITÉ PAR COMMUNE & PARAMÈTRE ---
@dlt.table(
    name="gold_qualite_commune_parametre",
    comment="Table de faits : qualité par commune, paramètre, année.",
    table_properties={"quality": "gold"}
)
def gold_qualite_commune_parametre():
    df = dlt.read("silver_mesures")

    group_cols = []
    for c in ["departement", "code_commune", "code_parametre", "annee"]:
        if c in df.columns:
            group_cols.append(c)

    agg_df = (
        df.groupBy(*group_cols)
          .agg(
              avg("resultat").alias("valeur_moyenne"),
              min("resultat").alias("valeur_min"),
              max("resultat").alias("valeur_max"),
              count("*").alias("nb_mesures")
          )
    )

    return agg_df
