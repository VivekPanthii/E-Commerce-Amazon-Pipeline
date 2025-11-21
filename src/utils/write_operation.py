from ..utils.spark_utils import get_spark_session,stop_spark
from pyspark.sql.functions import *
from pyspark.sql import DataFrame
from delta.tables import DeltaTable
from datetime import datetime
# today_date=datetime.today().strftime("%Y-%m-%d")
today_date= '2025-11-20'

spark=get_spark_session()
def write_to_append(df: DataFrame, path:str):

    try:
        if DeltaTable.isDeltaTable(spark,path):
            new_records=df
            new_count=new_records.count()

            if new_count>0:
                print(f"   ✓ Appending {new_count} new records ...")
                df=df.withColumn("_bronze_ingestion_date",lit(today_date))
                df.write\
                    .format("delta")\
                    .mode("append")\
                    .partitionBy("_bronze_ingestion_date")\
                    .option("mergeSchema",True)\
                    .save(path)
            else:
                print("No new records to append ...")

        else:
            print("    Delta table does not exist...")
            print(f"  ✓ First load, using append mode")
            df=df.withColumn("_bronze_ingestion_date",lit(today_date))
            df.write\
                    .format("delta")\
                    .mode("append")\
                    .partitionBy("_bronze_ingestion_date")\
                    .option("mergeSchema",True)\
                    .save(path)
    except Exception as e:
        print(f"    Somethings Went Wrongs!!! {e}")
