import dlt
from pyspark.sql.functions import col, trim, lower

@dlt.table(
    name="silver_analyses",
    comment="Table Silver nettoyée et normalisée.",
    table_properties={"quality": "silver"}
)
def silver_analyses():

    df = dlt.read("bronze_analyses")

    df_clean = (
        df.dropDuplicates()
          .na.drop(subset=["resultat"])
          .withColumn("resultat", col("resultat").cast("double"))
          .withColumn("libelle_parametre", trim(lower(col("libelle_parametre"))))
          .withColumn("code_commune", trim(col("code_commune")))
    )

    return df_clean
