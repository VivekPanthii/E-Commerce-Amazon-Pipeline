from src.utils.spark_utils import get_spark_session
from src.silver.silver_metadata import add_silver_metadata
from pyspark.sql import DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *
from typing import Dict, List, Tuple
from datetime import datetime
from pyspark.sql import Window
import uuid
from pyspark import StorageLevel

BRONZE_PATH_ORDER_ITEMS="./data/bronze/bronze_order_items_raw"

SILVER_PATH_ORDER_ITEMS="./data/silver/silver_order_items"
SILVER_ORDERS_PATH="./data/silver/silver_orders"

spark=get_spark_session()


def filter_invalid_data(df:DataFrame)->DataFrame:

    df=df.filter(
        (col("item_id").isNotNull() & (trim(col("item_id"))!="")) &
        (col("order_id").isNotNull() & (trim(col("order_id"))!="")) &
        (col("product_id").isNotNull() & (trim(col("product_id"))!=""))
    )

    return df


def standardize_product_fields(df: DataFrame) -> DataFrame:
    df = df.withColumn(
        "product_name_cleaned",
        when(
            col("product_name").isNotNull(),
            lower(trim(col("product_name")))
        ).otherwise(None)
    )

    df = df.withColumn(
        "category_cleaned",
        when(
            col("category").isNotNull(),
            lower(trim(col("category")))
        ).otherwise("uncategorized")
    )
    df = df.withColumn(
        "category_cleaned",
        initcap(
            when(col("category_cleaned").isin(['electronics', 'electronic', 'tech']), 'electronics')
            .when(col("category_cleaned").isin(['clothing', 'clothes', 'apparel']), 'clothing')
            .when(col("category_cleaned").isin(['home & garden', 'home', 'garden']), 'home_garden')
            .when(col("category_cleaned").isin(['books', 'book']), 'books')
            .when(col("category_cleaned").isin(['sports', 'sport', 'fitness']), 'sports')
            .when(col("category_cleaned").isin(['toys', 'toy']), 'toys')
            .otherwise(col("category_cleaned"))
        )
    )
    df = df.withColumn(
        "sku_cleaned",
        when(
            col("sku").isNotNull(),
            upper(trim(col("sku")))
        ).otherwise(None)
    )
    return df


def standardize_numeric_fields(df: DataFrame)-> DataFrame:
    print("  → Standardizing numeric fields...")
    df=df.withColumn(
        "quantity_cleaned",
        when(
            col("quantity").isNotNull(),
            col("quantity").cast("int")
        ).otherwise(0)
    )

    df=df.withColumn(
        "quantity_cleaned",
        when(
            col("quantity_cleaned") <=0,
            1
        ).otherwise(col("quantity_cleaned"))
    )


    df=df.withColumn(
        "unit_price_cleaned",
        when(
            col("unit_price").isNotNull(),
            col("unit_price").cast("decimal(10,2)")
        ).otherwise(lit(0.0).cast("decimal(10,2)"))
    )
    df = df.withColumn(
        "unit_price_cleaned",
        when(
            col("unit_price_cleaned") < 0,
            lit(0.0).cast("decimal(12,2)")
        ).otherwise(col("unit_price_cleaned"))
    )

    df = df.withColumn(
        "line_total_cleaned",
        when(
            col("line_total").isNotNull(),
            col("line_total").cast("decimal(12,2)")
        ).otherwise(lit(0.0).cast("decimal(12,2)"))
    )

    df = df.withColumn(
        "line_total_cleaned",
        when(
            col("line_total_cleaned") < 0,
            lit(0.0).cast("decimal(12,2)")
        ).otherwise(col("line_total_cleaned"))
    )
     
    return df

def validate_line_total(df: DataFrame)-> DataFrame:
    print("  → Validating line totals...")
    

    df = df.withColumn(
        "calculated_line_total",
        col("quantity_cleaned") * col("unit_price_cleaned")
    )
    

    df = df.withColumn(
        "line_total_consistent",
        abs(col("line_total_cleaned") - col("calculated_line_total")) <= 0.01
    )
    
    df = df.withColumn(
        "line_total_cleaned",
        when(
            col("line_total_consistent") == False,
            col("calculated_line_total")
        ).otherwise(col("line_total_cleaned"))
    )
    
    df = df.drop("calculated_line_total","line_total_consistent")
    
    return df

def deduplicate_order_items(df: DataFrame) -> DataFrame:

    print("  → Deduplicating records...")
    
    # Add row number partitioned by item_id
    window_spec = Window.partitionBy("item_id").orderBy(
        col("_bronze_ingestion_timestamp").desc()
    )
    
    df = df.withColumn("row_num", row_number().over(window_spec))
    
    # Count duplicates
    # total_before = df.count()
    
    # Keep only first record
    df_deduped = df.filter(col("row_num") == 1).drop("row_num")
    
    # total_after = df_deduped.count()
    # duplicates_removed = total_before - total_after
    
    print(f"      Removed duplicate records")
    
    return df_deduped


def filter_invalid_records(df: DataFrame) -> DataFrame:

    print("  → Filtering invalid records...")
    

    
    # Filter criteria:
    # 1. Must have valid item_id
    # 2. Must have valid order_id
    # 3. Must have valid product_id

    
    df_filtered = df.filter(
        # Valid item_id
        (col("item_id").isNotNull()) & 
        (trim(col("item_id")) != "") &
        
        # Valid order_id
        (col("order_id").isNotNull()) & 
        (trim(col("order_id")) != "") &
        
        # Valid product_id
        (col("product_id").isNotNull()) & 
        (trim(col("product_id")) != "") &
        
        
        # No critical DQ errors
        (
            (col("_dq_has_errors") == False) | 
            (col("_dq_error_count") == 0)
        )
    )
    
    
    print(f"      Filtered out invalid records")
    
    return df_filtered

def enrich_order_items(df: DataFrame) -> DataFrame:

    print("  → Enriching order items data...")
    
    # Item value category
    df = df.withColumn(
        "item_value_category",
        when(col("line_total_cleaned") >= 100, "high_value")
        .when(col("line_total_cleaned") >= 25, "medium_value")
        .otherwise("low_value")
    )
    
    # Quantity category
    df = df.withColumn(
        "quantity_category",
        when(col("quantity_cleaned") >= 5, "bulk")
        .when(col("quantity_cleaned") >= 2, "multiple")
        .otherwise("single")
    )
    
    # Price per unit category
    df = df.withColumn(
        "price_category",
        when(col("unit_price_cleaned") >= 100, "premium")
        .when(col("unit_price_cleaned") >= 25, "mid_range")
        .otherwise("budget")
    )
    
    # Is high quantity flag
    df = df.withColumn(
        "is_high_quantity",
        col("quantity_cleaned") >= 3
    )
    
    # Extended price (same as line_total but more descriptive)
    df = df.withColumn(
        "extended_price",
        col("line_total_cleaned")
    )
    
    # Average price per item (useful for bulk purchases)
    df = df.withColumn(
        "avg_price_per_item",
        (col("line_total_cleaned") / col("quantity_cleaned")).cast("decimal(10,2)")
    )
    
    return df

def validate_order_references(df: DataFrame) -> DataFrame:

    print("  → Validating order references...")
    
    try:
        # Read silver orders
        silver_orders = spark.read.format("delta").load(SILVER_ORDERS_PATH)
        valid_orders = silver_orders.select("order_id").distinct()
        
        # Left anti join to find orphaned items
        orphaned = df.join(
            valid_orders,
            "order_id",
            "left_anti"
        )
        
        orphaned_count = orphaned.count()
        
        # Add flag for valid order reference
        df = df.join(
            valid_orders.withColumn("has_valid_order", lit(True)),
            "order_id",
            "left"
        )
        
        df = df.withColumn(
            "has_valid_order",
            when(col("has_valid_order").isNull(), False).otherwise(True)
        )
        
        print(f"      Found {orphaned_count} orphaned items (no matching order)")
        
    except Exception as e:
        print(f"      ⚠️  Could not validate order references: {str(e)}")
        df = df.withColumn("has_valid_order", lit(True))
    
    return df


def calculate_order_aggregates(df: DataFrame) -> DataFrame:

    print("  → Calculating order aggregates...")
    
    # Calculate order-level metrics
    window_order = Window.partitionBy("order_id")
    
    df = df.withColumn(
        "order_item_count",
        count("item_id").over(window_order)
    )
    
    df = df.withColumn(
        "order_total_items_value",
        sum("line_total_cleaned").over(window_order)
    )
    
    df = df.withColumn(
        "item_pct_of_order",
        when(
            col("order_total_items_value") > 0,
            (col("line_total_cleaned") / col("order_total_items_value") * 100).cast("decimal(10,2)")
        ).otherwise(0.0)
    )
    
    # Rank items within order by value
    df = df.withColumn(
        "item_rank_in_order",
        row_number().over(
            Window.partitionBy("order_id").orderBy(col("line_total_cleaned").desc())
        )
    )
    
    # Is highest value item in order
    df = df.withColumn(
        "is_highest_value_item",
        col("item_rank_in_order") == 1
    )
    
    return df
def select_final_columns(df: DataFrame) -> DataFrame:

    return df.select(
        # Business Keys
        "item_id",
        "order_id",
        "product_id",
        col("sku_cleaned").alias("sku_cleaned"),
        
        # Cleaned Product Info
        col("product_name_cleaned").alias("product_name"),
        col("category_cleaned").alias("category"),
        
        # Cleaned Quantities and Prices
        col("quantity_cleaned").alias("quality"),
        col("unit_price_cleaned").alias("unit_price"),
        col("line_total_cleaned").alias("line_total"),
        "extended_price",
        "avg_price_per_item",
        
        # Derived Categories
        "item_value_category",
        "quantity_category",
        "price_category",
        "is_high_quantity",
        
        # Order-Level Aggregates
        "order_item_count",
        "order_total_items_value",
        "item_pct_of_order",
        "item_rank_in_order",
        "is_highest_value_item",
        
        # Data Quality Flags
        "has_valid_order",
        
        # Silver Metadata
        "_silver_processing_timestamp",
        "_silver_processing_date",
        "_silver_batch_id",
        "_silver_source_batch_id",
        "_silver_record_hash",
        
        # Bronze Reference (for lineage)
        "_bronze_ingestion_batch_id",
        "_bronze_ingestion_timestamp"
    )



class SilverOrderItemsPipeline:
    def __init__(self,metadata_manager=None):
        self.batch_id=str(uuid.uuid4())
        self.processing_start=datetime.now()
        self.stats={}
        self.metadata_manager=metadata_manager

    def run(self,bronze_batch_id: str=None):
        print("\n" + "="*70)
        print("SILVER LAYER TRANSFORMATION PIPELINE - ORDER ITEMS")
        print("="*70)
        print(f"Batch ID:    {self.batch_id}")
        print(f"Started:     {self.processing_start.strftime('%Y-%m-%d %H:%M:%S')}")
        if bronze_batch_id:
            print(f"Source Batch: {bronze_batch_id}")
        print("="*70)
        
        # ===== STEP 1: READ FROM BRONZE =====
        print("\n[1/10] Reading from bronze layer...")
        
        try:
            bronze_df = spark.read.format("delta").load(BRONZE_PATH_ORDER_ITEMS)
            
            # Filter by bronze_batch_id if specified
            if bronze_batch_id:
                bronze_df = bronze_df.filter(
                    col("_bronze_ingestion_batch_id") == bronze_batch_id
                )
            else:
                # Process only records not yet in silver
                try:
                    silver_df = spark.read.format("delta").load(SILVER_PATH_ORDER_ITEMS)
                    processed_batches = silver_df.select("_bronze_ingestion_batch_id").distinct()
                    
                    bronze_df = bronze_df.join(
                        processed_batches,
                        "_bronze_ingestion_batch_id",
                        "left_anti"
                    )
                except:
                    print("      No existing silver data, processing all bronze records")
            
            records_input = bronze_df.count()
            self.stats['records_input'] = records_input
            
            print(f"  ✓ Read {records_input:,} records from bronze")
            
            if records_input == 0:
                print("\n  ⚠️  No new records to process")
                return None, self.stats
            
        except Exception as e:
            print(f"  ❌ Error reading bronze: {str(e)}")
            raise
        
        # Store source batch ID
        source_batch_id = bronze_df.select("_bronze_ingestion_batch_id").first()[0]
        
        # ===== STEP 2: STANDARDIZATION =====
        print("\n[2/10] Standardizing data...")
        
        df = bronze_df
        df.persist(StorageLevel.MEMORY_AND_DISK)
        df = standardize_product_fields(df)
        df = standardize_numeric_fields(df)
        
        print("  ✓ Standardization complete")
        
        # ===== STEP 3: LINE TOTAL VALIDATION =====
        print("\n[3/10] Validating line totals...")
        
        df = validate_line_total(df)
        
        print("  ✓ Line total validation complete")
        
        # ===== STEP 4: DEDUPLICATION =====
        print("\n[4/10] Deduplicating records...")
        
        # records_before_dedup = df.count()
        df = deduplicate_order_items(df)
        # records_after_dedup = df.count()
        
        # self.stats['records_deduplicated'] = records_before_dedup - records_after_dedup
        
        print("  ✓ Deduplication complete")
        
        # ===== STEP 5: FILTER INVALID RECORDS =====
        print("\n[5/10] Filtering invalid records...")
        
        # records_before_filter = df.count()
        df = filter_invalid_records(df)
        # records_after_filter = df.count()
        
        # self.stats['records_filtered'] = records_before_filter - records_after_filter
        
        print("  ✓ Filtering complete")
        
        # ===== STEP 6: VALIDATE ORDER REFERENCES =====
        print("\n[6/10] Validating order references...")
        
        df = validate_order_references(df)
        
        print("  ✓ Reference validation complete")
        
        # ===== STEP 7: ENRICHMENT =====
        print("\n[7/10] Enriching order items data...")
        
        df = enrich_order_items(df)
        
        print("  ✓ Enrichment complete")
        
        # ===== STEP 8: CALCULATE ORDER AGGREGATES =====
        print("\n[8/10] Calculating order aggregates...")
        
        df = calculate_order_aggregates(df)
        
        print("  ✓ Aggregates calculated")
        
        # ===== STEP 9: ADD SILVER METADATA =====
        print("\n[9/10] Adding silver metadata...")
        
        df = add_silver_metadata(df, self.batch_id, source_batch_id)
        
        print("  ✓ Silver metadata added")
        
        # ===== STEP 10: SELECT FINAL COLUMNS & WRITE =====
        print("\n[10/10] Selecting final columns and writing to silver...")
        
        df_final = select_final_columns(df)
        records_output = df_final.count()
        
        self.stats['records_output'] = records_output
        
        # Write to Delta
        df_final.write \
            .format("delta") \
            .mode("append") \
            .partitionBy("_silver_processing_date") \
            .option("mergeSchema", "true") \
            .save(SILVER_PATH_ORDER_ITEMS)
        
        # Create table reference
        # try:
        #     self.spark.sql(f"""
        #         CREATE TABLE IF NOT EXISTS {SILVER_TABLE_ORDER_ITEMS}
        #         USING DELTA
        #         LOCATION '{SILVER_PATH_ORDER_ITEMS}'
        #     """)
        # except:
        #     pass
        
        print(f"  ✓ Written {records_output:,} records to silver")
        
        # ===== FINALIZE =====
        processing_end = datetime.now()
        duration = (processing_end - self.processing_start).total_seconds()
        
        self.stats['duration_seconds'] = duration
        self.stats['records_per_second'] = records_output / duration if duration > 0 else 0
        
        # Calculate data quality improvement
        dq_improvement = ((records_output / records_input) * 100) if records_input > 0 else 0
        self.stats['data_quality_score'] = dq_improvement
        
        # ===== LOG TO METADATA =====
        # if self.metadata_manager:
        #     print("\n[11/11] Logging to metadata tables...")
        #     try:
        #         self.metadata_manager.log_silver_batch(
        #             batch_id=self.batch_id,
        #             source_batch_id=source_batch_id,
        #             table_name=SILVER_TABLE_ORDER_ITEMS,
        #             records_input=records_input,
        #             records_output=records_output,
        #             records_filtered=self.stats['records_filtered'],
        #             records_deduplicated=self.stats['records_deduplicated'],
        #             records_standardized=records_output,
        #             transformation_rules=[
        #                 "product_field_standardization",
        #                 "numeric_standardization",
        #                 "line_total_validation",
        #                 "deduplication",
        #                 "invalid_record_filtering",
        #                 "order_reference_validation",
        #                 "item_enrichment",
        #                 "order_aggregate_calculation"
        #             ],
        #             processing_duration=duration,
        #             data_quality_score=dq_improvement
        #         )
        #         print("  ✓ Metadata logged")
        #     except Exception as e:
        #         print(f"  ⚠️  Metadata logging failed: {str(e)}")
        
        # ===== SUMMARY =====
        print("\n" + "="*70)
        print("TRANSFORMATION SUMMARY")
        print("="*70)
        print(f"Status:                ✅ SUCCESS")
        print(f"Duration:              {duration:.2f} seconds")
        print(f"Throughput:            {self.stats['records_per_second']:.0f} records/sec")
        print(f"Records input:         {records_input:,}")
        # print(f"Records deduplicated:  {self.stats['records_deduplicated']:,}")
        # print(f"Records filtered:      {self.stats['records_filtered']:,}")
        print(f"Records output:        {records_output:,}")
        print(f"Data retention:        {dq_improvement:.1f}%")
        print("="*70 + "\n")
        
        return df_final, self.stats
    

if __name__=="__main__":
    pipeline=SilverOrderItemsPipeline()
    df, stats=pipeline.run()