from pyspark.sql import DataFrame
def rename_columns(df: DataFrame, mapping: dict) -> DataFrame:
    """
    Renomme les colonnes d'un DataFrame Spark selon un mapping.
    mapping = {"ancienne_colonne": "nouvelle_colonne"}
    """
    for old, new in mapping.items():
        df = df.withColumnRenamed(old, new)
    return df
