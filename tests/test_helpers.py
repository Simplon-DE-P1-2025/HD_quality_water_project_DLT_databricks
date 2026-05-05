'''
 Test pour utils/helpers.py

Ce test vérifie que :

    rename_columns() renomme bien les colonnes

    le DataFrame Spark reste valide
    '''

    from utils.helpers import rename_columns
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

def test_rename_columns():
    df = spark.createDataFrame(
        [(1, 2)],
        ["old_col", "other"]
    )
    mapping = {"old_col": "new_col"}

    df2 = rename_columns(df, mapping)

    assert "new_col" in df2.columns
    assert "old_col" not in df2.columns

