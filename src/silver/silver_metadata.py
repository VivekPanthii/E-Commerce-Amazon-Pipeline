from pyspark.sql.functions import *
from pyspark.sql import DataFrame


def add_silver_metadata(df: DataFrame, batch_id: str, source_batch_id: str) -> DataFrame:
    df = df.withColumn("_silver_processing_timestamp", current_timestamp())
    df = df.withColumn("_silver_processing_date", current_date())
    df = df.withColumn("_silver_batch_id", lit(batch_id))
    df = df.withColumn("_silver_source_batch_id", lit(source_batch_id))
    df = df.withColumn("_silver_record_hash", 
                      md5(concat_ws("|", *[col(c) for c in df.columns if not c.startswith("_")])))
    
    return df