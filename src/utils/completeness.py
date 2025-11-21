from pyspark.sql import DataFrame
from pyspark.sql.functions import *


def completeness_final_set(df:DataFrame)-> DataFrame:
    all_fields=[c for c in df.columns if not c.startswith("_")]

    non_null_expr=lit(0)
    for c in all_fields:
        non_null_expr=non_null_expr+when(
            col(c).isNotNull() & 
            (trim(col(c))!=""),1
        ).otherwise(0)

    df=df.withColumn(
        "_dq_completeness_score",
        (non_null_expr/len(all_fields)*100).cast("decimal(5,2)")
    )

    # ========== SET FINAL DQ FLAGS ==========
    df = df.withColumn("_dq_has_errors", size(col("_dq_errors")) > 0)
    df = df.withColumn("_dq_error_count", size(col("_dq_errors")))
    df = df.withColumn("_dq_warning_count", size(col("_dq_warnings")))
    
    # Convert arrays to JSON strings
    df = df.withColumn("_dq_error_details", to_json(col("_dq_errors")))
    df = df.withColumn("_dq_warning_details", to_json(col("_dq_warnings")))
    
    # Clean up temporary columns
    df = df.drop("_dq_errors", "_dq_warnings")
    return df