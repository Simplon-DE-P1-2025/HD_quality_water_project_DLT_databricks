import yaml
from typing import Dict, Any, List
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lower, regexp_replace, trim


# ============================================================
# CONFIG
# ============================================================

def load_config(path: str) -> Dict[str, Any]:
    """
    Charge le fichier config.yaml depuis Databricks Repos ou VS Code.
    """
    with open(path, "r") as f:
        return yaml.safe_load(f)


# ============================================================
# LOGGING
# ============================================================

def log(msg: str):
    """
    Simple logger compatible Databricks / VS Code.
    """
    print(f"[INFO] {msg}")


# ============================================================
# DATAFRAME HELPERS
# ============================================================

def list_to_df(spark, data: List[Dict[str, Any]]) -> DataFrame:
    """
    Convertit une liste de dictionnaires (API Hub'Eau) en DataFrame Spark.
    """
    if not data:
        return spark.createDataFrame([], schema=None)
    return spark.createDataFrame(data)


def standardize_column_names(df: DataFrame) -> DataFrame:
    """
    Nettoie les noms de colonnes :
      - minuscules
      - remplace espaces par _
      - supprime caractères spéciaux
    """
    for c in df.columns:
        new_c = (
            c.lower()
            .replace(" ", "_")
            .replace("-", "_")
            .replace("é", "e")
            .replace("è", "e")
            .replace("ê", "e")
            .replace("à", "a")
        )
        df = df.withColumnRenamed(c, new_c)
    return df


def clean_string_columns(df: DataFrame) -> DataFrame:
    """
    Nettoie les colonnes string :
      - trim
      - suppression des espaces multiples
    """
    for c, t in df.dtypes:
        if t == "string":
            df = df.withColumn(c, trim(regexp_replace(col(c), r"\s+", " ")))
    return df


def enforce_schema(df: DataFrame, schema: Dict[str, str]) -> DataFrame:
    """
    Force un schéma simple (types Spark) sur un DataFrame.
    Exemple schema = {"annee": "int", "resultat": "double"}
    """
    for col_name, col_type in schema.items():
        if col_name in df.columns:
            df = df.withColumn(col_name, col(col_name).cast(col_type))
    return df


# ============================================================
# STORAGE HELPERS
# ============================================================

def get_storage_paths(config: Dict[str, Any]) -> Dict[str, str]:
    """
    Retourne les bons chemins selon le mode (local VS Code ou azure Databricks).
    """
    mode = config["storage"]["mode"]

    if mode == "local":
        return config["storage"]["local"]

    if mode == "azure":
        base = config["storage"]["azure"]["mount_point"]
        return {
            "base_path": base,
            "bronze_path": f"{base}/bronze",
            "silver_path": f"{base}/silver",
            "gold_path": f"{base}/gold",
            "logs_path": f"{base}/logs",
        }

    raise ValueError(f"Mode de stockage inconnu : {mode}")


def write_delta(df: DataFrame, path: str, mode: str = "overwrite", partition_by=None):
    """
    Écriture Delta Lake générique.
    """
    if partition_by:
        df.write.format("delta").mode(mode).partitionBy(partition_by).save(path)
    else:
        df.write.format("delta").mode(mode).save(path)


# ============================================================
# BRONZE / SILVER / GOLD HELPERS
# ============================================================

def write_bronze(df: DataFrame, config: Dict[str, Any], table_name: str, partition_col: str = None):
    paths = get_storage_paths(config)
    path = f"{paths['bronze_path']}/{table_name}"
    log(f"Écriture Bronze → {path}")
    write_delta(df, path, mode="overwrite", partition_by=partition_col)


def write_silver(df: DataFrame, config: Dict[str, Any], table_name: str, partition_col: str = None):
    paths = get_storage_paths(config)
    path = f"{paths['silver_path']}/{table_name}"
    log(f"Écriture Silver → {path}")
    write_delta(df, path, mode="overwrite", partition_by=partition_col)


def write_gold(df: DataFrame, config: Dict[str, Any], table_name: str, partition_col: str = None):
    paths = get_storage_paths(config)
    path = f"{paths['gold_path']}/{table_name}"
    log(f"Écriture Gold → {path}")
    write_delta(df, path, mode="overwrite", partition_by=partition_col)
