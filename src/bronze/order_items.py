from ..utils.spark_utils import get_spark_session
from pyspark.sql import DataFrame
from datetime import datetime
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.functions import try_to_timestamp
import uuid
from ..utils.change_detect import handle_schema_changes
from ..utils.duplicates_detect import detect_duplicates_bronze
from ..utils.write_operation import write_to_append
from ..utils.completeness import completeness_final_set
from ..utils.metadata import enriched_metadata
from typing import Tuple, List, Dict

today_date=datetime.today().strftime("%Y-%m-%d")
BRONZE_TABLE="bronze_order_items_raw"
BRONZE_ROOT_PATH=f"./data/bronze/bronze_order_items_raw"
SOURCE_PATH=f"./data/landing/orders_item/order_items.csv"

spark= get_spark_session()

EXPECTED_SCHEMA_ORDER_ITEMS_V1 = {
    "version": "1.0",
    "fields": [
        {"name": "item_id", "type": "string", "required": True},
        {"name": "order_id", "type": "string", "required": True},
        {"name": "product_id", "type": "string", "required": True},
        {"name": "product_name", "type": "string", "required": False},
        {"name": "category", "type": "string", "required": False},
        {"name": "quantity", "type": "integer", "required": True},
        {"name": "unit_price", "type": "float", "required": True},
        {"name": "line_total", "type": "float", "required": True},
        {"name": "sku", "type": "string", "required": False}
    ]
}



# ============================================================================
# DATA QUALITY VALIDATION (NON-INTRUSIVE)
# ============================================================================ 

@udf(ArrayType(StringType()))
def validate_required_fields_udf(item_id:str,order_id:str,product_id:str)->List:
    errors=[]

    def is_empty(value):
        return value is None or str(value).strip()==""
    if is_empty(item_id):
        errors.append("MISSING_REQUIRED_FIELD:item_id")
    if is_empty(order_id):
        errors.append("MISSING_REQUIRED_FIELD:order_id")
    if is_empty(product_id):
        errors.append("MISSING_REQUIRED_FIELD:product_id")
    return errors


@udf(ArrayType(StringType()))
def validate_numeric_fields_udf(quantity:int, unit_price:float, line_total:float)->List:
    errors=[]
    fields={
        'quantity':quantity,
        'unit_price':unit_price,
        'line_total':line_total
    }


    for field_name,value in fields.items():
        if value is not None and str(value).strip()!="":
            try:
                num_val=float(value)
                if num_val<0:
                    errors.append(f"NEGATIVE_VALUE:{field_name}")
            except (ValueError,TypeError):
                errors.append(f"INVALID_NUMERIC: {field_name}")
    return errors

def validate_orders_item_bronze_data(df:DataFrame)->DataFrame:
    df=df.withColumn(
        "_dq_errors",
        validate_required_fields_udf(
            col("item_id"),
            col("order_id"),
            col("product_id")
        )
    )

    df=df.withColumn(
        "_dq_numeric_errors",
        validate_numeric_fields_udf(
            col("quantity"),
            col("unit_price"),
            col("line_total")
        )
    )

    df=df.withColumn(
        "_dq_errors",
        array_union(col("_dq_errors"),col("_dq_numeric_errors"))
    ).drop("_dq_numeric_errors")

    df = df.withColumn("_dq_warnings", array())
    df=completeness_final_set(df)
    return df


class BronzePipeline:
    def __init__(self,metadata_manager=None):
        self.batch_id=str(uuid.uuid4())
        self.ingestion_start=datetime.now()
        self.stats={}
        
    def run(self,source_file_path:str, write_mode:str):
        print("\n" + "="*70)
        print("BRONZE LAYER INGESTION PIPELINE - ORDER ITEMS")
        print("="*70)
        print(f"Batch ID:    {self.batch_id}")
        print(f"Source:      {source_file_path}")
        print(f"Write Mode:  {write_mode}")
        print(f"Started:     {self.ingestion_start.strftime('%Y-%m-%d %H:%M:%S')}")
        print("="*70)       
        print("\n[1/7] Reading Source Data...")
        df=spark\
            .read\
                .option("header",True)\
                    .csv(SOURCE_PATH)
        source_count=df.count()

        self.stats["source_count"]=source_count
        print(f"  ✓ Read {source_count:,} records from source")


        print("[2/7] Detecting Schema Changes...")
        df,schema_changes=handle_schema_changes(df,EXPECTED_SCHEMA_ORDER_ITEMS_V1)
        self.stats["schema_changes"]=schema_changes


        print("[3/7] Running Data Quality Checks...")
        df=validate_orders_item_bronze_data(df)

        error_count=df.filter(col("_dq_has_errors")==True).count()
        total_count=df.count()
        dq_score=((total_count-error_count)/total_count * 100) if total_count>0 else 0

        self.stats["error_count"]=error_count
        self.stats["dq_score"]=dq_score

        print(f"  ✓ Validation complete")
        print("[4/7] Detecting Duplicates...")
        df=detect_duplicates_bronze(df,BRONZE_ROOT_PATH)
        dup_count=df.filter(col("_dq_is_duplicate")).count()
        self.stats["dup_count"]=dup_count
        print(f"  ✓ Duplicates detected: {dup_count:,}")

        print("[5/7] Enriching with metadata...")
        df=enriched_metadata(df,source_file_path,self.batch_id)
        print(f"  ✓ Metadata enrichment complete")

        print(f"\n[6/7] Writing to bronze layer (mode: {write_mode})...")
        write_to_append(df,BRONZE_ROOT_PATH)    
        print(f"  ✓ Customer Bronze Layer complete")


        print("\n[7/7] Finalizing and logging statistics...")
        ingestion_end=datetime.now()
        duration=(ingestion_end-self.ingestion_start).total_seconds()

        self.stats["duration"]=duration
        self.stats["records_per_second"]=source_count/duration if duration > 0 else 0

        # try:
        #     spark.sql(f"""
        #         CREATE TABLE IF NOT EXISTS {BRONZE_TABLE}
        #         USING DELTA
        #         LOCATION '{BRONZE_ROOT_PATH}'
        #     """)
        # except:
        #     pass
        
        print(f"  ✓ Statistics logged")
        
        # SUMMARY
        print("\n" + "="*70)
        print("INGESTION SUMMARY")
        print("="*70)
        print(f"Status:              ✅ SUCCESS")
        print(f"Duration:            {duration:.2f} seconds")
        print(f"Throughput:          {self.stats['records_per_second']:.0f} records/sec")
        print(f"Records processed:   {total_count:,}")
        print(f"Records with errors: {error_count:,} ({error_count/total_count*100:.2f}%)")
        print(f"Duplicates detected: {dup_count:,}")
        print(f"Data quality score:  {dq_score:.2f}%")
        print(f"Schema changes:      {'Yes' if schema_changes['has_changes'] else 'No'}")
        print("="*70 + "\n")

        return df, self.stats
    
if __name__ =="__main__":
    pipeline=BronzePipeline()
    df_bronze,stats=pipeline.run(
        source_file_path=SOURCE_PATH,
        write_mode="append"
    )