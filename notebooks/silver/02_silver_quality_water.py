
import dlt
from pyspark.sql.functions import col, to_timestamp
from utils.helpers import rename_columns
import yaml
# --- MOCK DLT FOR CI ENVIRONMENT ---
try:
    import dlt
except ImportError:
    class MockDLT:
        def table(self, *args, **kwargs):
            def decorator(func):
                return func
            return decorator

        def view(self, *args, **kwargs):
            def decorator(func):
                return func
            return decorator

    dlt = MockDLT()
# -----------------------------------

# --- Chargement du fichier YAML ---
with open("config/config.yaml", "r") as f:
    cfg = yaml.safe_load(f)

# Ingestion (pas utilisé directement ici, mais chargé pour cohérence)
ANNEES = cfg["ingestion"]["annees"]
DEPARTEMENT = cfg["ingestion"]["departement"]
API_BASE_URL = cfg["ingestion"]["api_base_url"]

# Storage (pas utilisé ici, mais cohérent avec Bronze/Gold)
STORAGE_MODE = cfg["storage"]["mode"]
BRONZE_PATH = cfg["storage"][STORAGE_MODE]["bronze_path"]


@dlt.table(
    name="silver_analyses",
    comment="Table contenant les données d'analyses nettoyées et standardisées."
)
def silver_analyses():
    """
    Nettoyage des données Bronze :
    - cast des types
    - normalisation des noms de colonnes
    - suppression des doublons
    """
    df = dlt.read("bronze_analyses")

    # Cast des types
    if "resultat_numerique" in df.columns:
        df = df.withColumn("resultat_numerique", col("resultat_numerique").cast("double"))

    if "date_prelevement" in df.columns:
        df = df.withColumn("date_prelevement", to_timestamp("date_prelevement"))

    # Suppression des doublons
    df = df.dropDuplicates()

    # Normalisation des noms de colonnes
    mapping = {}
    if "code_departement" in df.columns:
        mapping["code_departement"] = "departement"
    if "code_commune" in df.columns:
        mapping["code_commune"] = "commune"

    if mapping:
        df = rename_columns(df, mapping)

    return df
