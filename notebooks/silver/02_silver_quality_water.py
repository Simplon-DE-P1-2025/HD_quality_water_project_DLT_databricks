import dlt
from pyspark.sql.functions import col, to_timestamp
from utils.helpers import rename_columns


@dlt.table(
    name="silver_analyses",
    comment="Données d'analyses nettoyées et standardisées."
)
def silver_analyses():
    """
    Nettoyage des données Bronze :
    - cast des types
    - normalisation des noms de colonnes
    - suppression des doublons
    """
    df = dlt.read("bronze_analyses")

    # Cast de quelques colonnes typiques (adaptable selon le schéma réel)
    if "resultat_numerique" in df.columns:
        df = df.withColumn("resultat_numerique", col("resultat_numerique").cast("double"))

    if "date_prelevement" in df.columns:
        df = df.withColumn("date_prelevement", to_timestamp("date_prelevement"))

    df = df.dropDuplicates()

    mapping = {}
    if "code_departement" in df.columns:
        mapping["code_departement"] = "departement"
    if "code_commune" in df.columns:
        mapping["code_commune"] = "commune"

    if mapping:
        df = rename_columns(df, mapping)

    return df
