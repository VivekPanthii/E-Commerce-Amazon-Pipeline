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


today_date=datetime.today().strftime("%Y-%m-%d")
BRONZE_TABLE="bronze_orders_raw"
BRONZE_ROOT_PATH=f"./data/bronze/bronze_orders_raw"
SOURCE_PATH_ORDER="./data/landing/orders/orders.csv"
# source_file_path=f"./data/landing/orders/orders.csv"

spark=get_spark_session()
EXPECTED_SCHEMA_ORDERS_V1 = {
    "version": "1.0",
    "fields": [
        {"name": "order_id", "type": "string", "required": True},
        {"name": "customer_id", "type": "string", "required": True},
        {"name": "order_date", "type": "string", "required": True},
        {"name": "subtotal", "type": "float", "required": False},
        {"name": "tax_amount", "type": "float", "required": False},
        {"name": "discount_amount", "type": "float", "required": False},
        {"name": "total_amount", "type": "float", "required": False},
        {"name": "payment_method", "type": "string", "required": False},
        {"name": "created_at", "type": "string", "required": True}
    ]
}



# ============================================================================
# DATA QUALITY VALIDATION (NON-INTRUSIVE)
# ============================================================================ 

@udf(ArrayType(StringType()))
def validate_required_fields_udf(order_id:str, customer_id:str, order_date:str, created_at:str):

    errors = []
    
    def is_empty(value):
        return value is None or str(value).strip() == ""
    
    if is_empty(order_id):
        errors.append("MISSING_REQUIRED_FIELD:order_id")
    if is_empty(customer_id):
        errors.append("MISSING_REQUIRED_FIELD:customer_id")
    if is_empty(order_date):
        errors.append("MISSING_REQUIRED_FIELD:order_date")
    if is_empty(created_at):
        errors.append("MISSING_REQUIRED_FIELD:created_at")
    
    return errors


@udf(ArrayType(StringType()))
def validate_numeric_fields_udf(total_amount, subtotal, tax_amount, discount_amount):

    errors = []
    
    fields = {
        'total_amount': total_amount,
        'subtotal': subtotal,
        'tax_amount': tax_amount,
        'discount_amount': discount_amount
    }
    
    for field_name, value in fields.items():
        if value is not None and str(value).strip() != "":
            try:
                num_val = float(value)
                if num_val < 0:
                    errors.append(f"NEGATIVE_AMOUNT:{field_name}")
            except (ValueError, TypeError):
                errors.append(f"INVALID_NUMERIC:{field_name}")
    
    return errors


def validate_orders_bronze_data(df: DataFrame) -> DataFrame:
    """
    Optimized validation using UDFs - avoids code generation issues
    """
    
    df = df.withColumn(
        "_dq_errors",
        validate_required_fields_udf(
            col("order_id"),
            col("customer_id"),
            col("order_date"),
            col("created_at")
        )
    )
    
    df = df.withColumn(
        "_dq_numeric_errors",
        validate_numeric_fields_udf(
            col("total_amount"),
            col("subtotal"),
            col("tax_amount"),
            col("discount_amount")
        )
    )
    
    # Merge errors
    df = df.withColumn(
        "_dq_errors",
        array_union(col("_dq_errors"), col("_dq_numeric_errors"))
    ).drop("_dq_numeric_errors")
    
    # Empty warnings for now
    df = df.withColumn("_dq_warnings", array())
    
    # Calculate completeness
    df = completeness_final_set(df)
    return df

# ============================================================================
# MAIN PIPELINE CLASS
# ============================================================================


class BronzePipeline:

    def __init__(self,metadata_manager=None):
        self.batch_id=str(uuid.uuid4())
        self.ingestion_start=datetime.now()
        self.stats={}
        # self.metadata_manager=metadata_manager

    def run(self,source_file_path:str, write_mode:str):
        print("\n" + "="*70)
        print("BRONZE LAYER INGESTION PIPELINE - ORDERS")
        print("="*70)
        print(f"Batch ID:    {self.batch_id}")
        print(f"Source:      {source_file_path}")
        print(f"Write Mode:  {write_mode}")
        print(f"Started:     {self.ingestion_start.strftime('%Y-%m-%d %H:%M:%S')}")
        print("="*70)


        # For the manual run
        # customers_df = generate_customers(2000)
        # customers_df.to_csv(f'{CUSTOMER_PATH}/customers.csv', index=False)

        print("\n[1/7] Reading Source Data...")
        df=spark\
            .read\
                .option("header",True)\
                    .csv(SOURCE_PATH_ORDER)
        source_count=df.count()

        self.stats["source_count"]=source_count
        print(f"  ✓ Read {source_count:,} records from source")


        print("[2/7] Detecting Schema Changes...")
        df,schema_changes=handle_schema_changes(df,EXPECTED_SCHEMA_ORDERS_V1)
        self.stats["schema_changes"]=schema_changes


        print("[3/7] Running Data Quality Checks...")
        df=validate_orders_bronze_data(df)

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
        #         CREATE TABLE IF NOT EXISTS {BRONZE_TABLE_CUSTOMERS}
        #         USING DELTA
        #         LOCATION '{BRONZE_PATH_CUSTOMERS}'
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

if __name__=="__main__":

    # print("\nInitializing Metadata Manager...")
    # from metadata_manager import MetadataManager  # Import the metadata manager
    
    # metadata_mgr = MetadataManager(
    #     spark=spark,
    #     metadata_path="/tmp/metadata"  # Change to your metadata path
    # )

    pipeline= BronzePipeline()
    df_bronze, stats=pipeline.run(
        source_file_path=SOURCE_PATH_ORDER,
        write_mode="append"
    )






    #========= DATE VALIDATION ==========

    # df = df.withColumn(
    #     "_temp_parsed_order_date",
    #     coalesce(
    #         try_to_timestamp(col("order_date"), "yyyy-MM-dd HH:mm:ss")
    #     )
    # )


    # df=df.withColumn(
    #     "_dq_errors",
    #     when(
    #         col("_temp_parsed_order_date").isNull() &
    #         col("order_date").isNotNull(),
    #         array_union(
    #             col("_dq_errors"),
    #             array(
    #                 concat(lit("INVALID_DATE_FORMAT:"),col('order_date').cast("string")))
    #         )
    #     )
    # )
    #========= STATUS VALIDATION ==========

    # validation_status=["pending", "confirmed", "processing", "shipped", 
    #                  "delivered", "cancelled", "returned"]
    # df=df.withColumn(
    #     "_dq_warnings",
    #     when(
    #         ~lower(trim(col("order_status"))).isin(validation_status) &
    #         col("order_status").isNotNull(),
    #         array_union(
    #             col("_dq_warnings"),
    #             array(
    #                 concat(
    #                     lit("UNKNOWN_STATUS:"),
    #                         col("order_status")))
    #         )
    #     ).otherwise(col("_dq_warnings"))
    # )