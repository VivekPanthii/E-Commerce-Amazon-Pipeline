
from pyspark.sql import DataFrame
from pyspark.sql.functions import *
from pyspark.sql.window import Window
import hashlib

def enriched_metadata(df: DataFrame,source_file:str,batch_id:str)->DataFrame:

    df=df.withColumn("_bronze_ingestion_timestamp",current_timestamp())
    df=df.withColumn("_bronze_source_file",lit(source_file))
    df=df.withColumn("_bronze_ingestion_batch_id",lit(batch_id))
    df=df.withColumn("_bronze_ingestion_date",current_date())

    window_spec= Window.orderBy(monotonically_increasing_id())

    df=df.withColumn("_bronze_source_line_number",row_number().over(window_spec))

    all_source_cols=[c for c in df.columns if not c.startswith("_")]

    df=df.withColumn(
        "_bronze_raw_record",
        to_json(struct(*all_source_cols))
    )

    """source_file.encode() → gives a bytes object.

       Then you immediately call .hexdigest() on it → bytes objects don’t have .hexdigest(), hence the AttributeError.

       The .hexdigest() method belongs to the hash object returned by hashlib.md5(), not to bytes."""
    # file_hash=hashlib.md5(source_file.encode().hexdigest())
    file_hash=hashlib.md5(source_file.encode()).hexdigest()

    df=df.withColumn("_bronze_file_hash",lit(file_hash))

    df=df.withColumn("_schema_version",lit("1.0"))
    return df