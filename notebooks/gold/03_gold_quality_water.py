import yaml

with open("config/config.yaml", "r") as f:
    cfg = yaml.safe_load(f)

# Ingestion
ANNEES = cfg["ingestion"]["annees"]
DEPARTEMENT = cfg["ingestion"]["departement"]
API_BASE_URL = cfg["ingestion"]["api_base_url"]

# Storage
STORAGE_MODE = cfg["storage"]["mode"]
BRONZE_PATH = cfg["storage"][STORAGE_MODE]["bronze_path"]

@dlt.table(
    name="gold_qualite_commune_parametre",
    comment="Table contenant les indicateurs de qualité de l'eau par commune et paramètre."
)
def gold_qualite_commune_parametre():
    """
    Table d'indicateurs agrégés :
    - moyenne, min, max, nombre de mesures
    - par année, département, commune, paramètre
    """
    df = dlt.read("silver_analyses")

    group_cols = []
    for col_name in ["annee", "departement", "commune", "code_parametre"]:
        if col_name in df.columns:
            group_cols.append(col_name)

    if "resultat_numerique" not in df.columns or not group_cols:
        return df.limit(0)

    return (
        df.groupBy(*group_cols)
          .agg(
              avg("resultat_numerique").alias("valeur_moyenne"),
              min("resultat_numerique").alias("valeur_min"),
              max("resultat_numerique").alias("valeur_max"),
              count("*").alias("nb_mesures")
          )
    )
