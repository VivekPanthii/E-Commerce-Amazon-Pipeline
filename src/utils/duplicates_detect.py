from ..utils.spark_utils import get_spark_session
from pyspark.sql.functions import *
from pyspark.sql import DataFrame


spark=get_spark_session()
def detect_duplicates_bronze(df:DataFrame, table_name:str)-> DataFrame:
    data_columns=[c for c in df.columns if not c.startswith("_")]


    df=df.withColumn(
        "_bronze_record_hash",
        md5(concat_ws("|", *[coalesce(col(c).cast("string"),lit("NULL")) for c in data_columns]))
    )

    try:
        existing_bronze=spark.read.format("delta").load(table_name)

        existing_hash=existing_bronze.select(
            "_bronze_record_hash",
            "_bronze_ingestion_batch_id"
        ).distinct()


        df=df.join(existing_hash.alias("existing"),
                   df["_bronze_record_hash"]==existing_hash["_bronze_record_hash"],
                   "left"
        )
        df=df.withColumn(
            "_dq_is_duplicate",
            when(
                col("existing._bronze_ingestion_batch_id").isNotNull(),True
            ).otherwise(False)
        )

        df=df.withColumn(
            "_dq_duplicate_of_batch",
            col("existing._bronze_ingestion_batch_id")
        )


        df=df.drop(col("existing._bronze_record_hash"))


        

    except Exception as e:
        print(f"    Bronze table doesn't exist yet or error: {str(e)}")
        df = df.withColumn("_dq_is_duplicate", lit(False))
        df = df.withColumn("_dq_duplicate_of_batch", lit(None).cast("string"))
    
    return df