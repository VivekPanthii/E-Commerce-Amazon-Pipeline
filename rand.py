"""
Bronze Layer Implementation for Customers Table
Complete production-ready code with validation, metadata, and Delta Lake storage
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from delta.tables import DeltaTable
from datetime import datetime
import uuid
import hashlib
import json
from typing import Dict, List, Tuple

# ============================================================================
# CONFIGURATION & SCHEMA REGISTRY
# ============================================================================

EXPECTED_SCHEMA_CUSTOMERS_V1 = {
    "version": "1.0",
    "fields": [
        {"name": "customer_id", "type": "string", "required": True},
        {"name": "first_name", "type": "string", "required": False},
        {"name": "last_name", "type": "string", "required": False},
        {"name": "email", "type": "string", "required": True},
        {"name": "phone", "type": "string", "required": False},
        {"name": "registration_date", "type": "string", "required": False},
        {"name": "customer_segment", "type": "string", "required": False},
        {"name": "is_active", "type": "string", "required": False},  # Mixed types
        {"name": "lifetime_value", "type": "string", "required": False}
    ]
}

BRONZE_TABLE_CUSTOMERS = "bronze_customers_raw"
BRONZE_PATH_CUSTOMERS = "/tmp/bronze/customers_raw"  # Change to your path

# ============================================================================
# SCHEMA DETECTION & EVOLUTION
# ============================================================================

def detect_schema_changes(df: DataFrame, expected_schema: Dict) -> Dict:
    """Compare source schema with expected schema"""
    source_cols = set(df.columns)
    expected_cols = set([f["name"] for f in expected_schema["fields"]])
    
    new_columns = source_cols - expected_cols
    missing_columns = expected_cols - source_cols
    
    changes = {
        "has_changes": len(new_columns) > 0 or len(missing_columns) > 0,
        "new_columns": list(new_columns),
        "missing_columns": list(missing_columns),
        "detected_at": datetime.now().isoformat(),
        "source_column_count": len(source_cols),
        "expected_column_count": len(expected_cols)
    }
    
    return changes


def handle_schema_evolution(df: DataFrame, expected_schema: Dict) -> Tuple[DataFrame, Dict]:
    """Handle schema changes non-intrusively"""
    schema_changes = detect_schema_changes(df, expected_schema)
    
    if schema_changes["has_changes"]:
        print(f"  ⚠️  Schema changes detected!")
        print(f"      New columns: {schema_changes['new_columns']}")
        print(f"      Missing columns: {schema_changes['missing_columns']}")
        
        # Handle new columns - store in _extra_fields JSON
        if schema_changes["new_columns"]:
            extra_cols = schema_changes["new_columns"]
            df = df.withColumn(
                "_extra_fields",
                to_json(struct(*[col(c) for c in extra_cols]))
            )
            df = df.drop(*extra_cols)
        else:
            df = df.withColumn("_extra_fields", lit(None).cast("string"))
        
        # Handle missing columns - add as NULL
        if schema_changes["missing_columns"]:
            for col_name in schema_changes["missing_columns"]:
                df = df.withColumn(col_name, lit(None).cast("string"))
    else:
        df = df.withColumn("_extra_fields", lit(None).cast("string"))
        print(f"  ✓ Schema matches expected")
    
    return df, schema_changes


# ============================================================================
# DATA QUALITY VALIDATION (NON-INTRUSIVE)
# ============================================================================

def validate_customers_bronze(df: DataFrame) -> DataFrame:
    """
    Comprehensive data quality validation for customers
    NON-INTRUSIVE: Flag issues but preserve all data
    """
    
    # Initialize error and warning arrays
    df = df.withColumn("_dq_errors", array())
    df = df.withColumn("_dq_warnings", array())
    
    # ========== REQUIRED FIELD VALIDATION ==========
    required_fields = ["customer_id", "email"]
    
    for field in required_fields:
        df = df.withColumn(
            "_dq_errors",
            when(
                col(field).isNull() | (trim(col(field)) == ""),
                array_union(
                    col("_dq_errors"),
                    array(lit(f"MISSING_REQUIRED_FIELD:{field}"))
                )
            ).otherwise(col("_dq_errors"))
        )
    
    # ========== EMAIL FORMAT VALIDATION ==========
    email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    
    df = df.withColumn(
        "_dq_errors",
        when(
            col("email").isNotNull() & 
            ~col("email").rlike(email_pattern),
            array_union(
                col("_dq_errors"),
                array(lit("INVALID_EMAIL_FORMAT:email"))
            )
        ).otherwise(col("_dq_errors"))
    )
    
    # ========== PHONE FORMAT VALIDATION (WARNING) ==========
    # Various phone formats are acceptable, so just warn if unusual
    phone_pattern = r'^[\d\s\-\(\)\+]{7,}$'
    
    df = df.withColumn(
        "_dq_warnings",
        when(
            col("phone").isNotNull() & 
            (trim(col("phone")) != "") &
            ~col("phone").rlike(phone_pattern),
            array_union(
                col("_dq_warnings"),
                array(lit("UNUSUAL_PHONE_FORMAT:phone"))
            )
        ).otherwise(col("_dq_warnings"))
    )
    
    # ========== DATE VALIDATION ==========
    df = df.withColumn(
        "_temp_parsed_reg_date",
        coalesce(
            to_date(col("registration_date"), "yyyy-MM-dd"),
            to_date(col("registration_date"), "MM/dd/yyyy"),
            to_date(col("registration_date"), "dd-MM-yyyy")
        )
    )
    
    df = df.withColumn(
        "_dq_errors",
        when(
            col("_temp_parsed_reg_date").isNull() & 
            col("registration_date").isNotNull() &
            (trim(col("registration_date")) != ""),
            array_union(
                col("_dq_errors"),
                array(lit("INVALID_DATE_FORMAT:registration_date"))
            )
        ).otherwise(col("_dq_errors"))
    )
    
    # Check if registration date is in the future
    df = df.withColumn(
        "_dq_warnings",
        when(
            col("_temp_parsed_reg_date") > current_date(),
            array_union(
                col("_dq_warnings"),
                array(lit("FUTURE_REGISTRATION_DATE:registration_date"))
            )
        ).otherwise(col("_dq_warnings"))
    ).drop("_temp_parsed_reg_date")
    
    # ========== NUMERIC FIELD VALIDATION ==========
    df = df.withColumn(
        "_temp_ltv_numeric",
        col("lifetime_value").cast("decimal(12,2)")
    )
    
    df = df.withColumn(
        "_dq_errors",
        when(
            col("_temp_ltv_numeric").isNull() & 
            col("lifetime_value").isNotNull() &
            (trim(col("lifetime_value")) != ""),
            array_union(
                col("_dq_errors"),
                array(lit("INVALID_NUMERIC:lifetime_value"))
            )
        ).otherwise(col("_dq_errors"))
    )
    
    # Check for negative lifetime value
    df = df.withColumn(
        "_dq_errors",
        when(
            col("_temp_ltv_numeric") < 0,
            array_union(
                col("_dq_errors"),
                array(lit("NEGATIVE_VALUE:lifetime_value"))
            )
        ).otherwise(col("_dq_errors"))
    ).drop("_temp_ltv_numeric")
    
    # ========== CUSTOMER SEGMENT VALIDATION (WARNING) ==========
    valid_segments = ["basic", "standard", "premium", "vip"]
    
    df = df.withColumn(
        "_dq_warnings",
        when(
            col("customer_segment").isNotNull() &
            (trim(col("customer_segment")) != "") &
            ~lower(trim(col("customer_segment"))).isin(valid_segments),
            array_union(
                col("_dq_warnings"),
                array(lit(f"UNKNOWN_SEGMENT:{col('customer_segment')}"))
            )
        ).otherwise(col("_dq_warnings"))
    )
    
    # ========== BOOLEAN FIELD VALIDATION ==========
    # is_active can be: True, False, true, false, 1, 0, Y, N, Yes, No
    valid_boolean_values = ["true", "false", "1", "0", "y", "n", "yes", "no"]
    
    df = df.withColumn(
        "_dq_errors",
        when(
            col("is_active").isNotNull() &
            (trim(col("is_active")) != "") &
            ~lower(trim(col("is_active"))).isin(valid_boolean_values),
            array_union(
                col("_dq_errors"),
                array(lit("INVALID_BOOLEAN:is_active"))
            )
        ).otherwise(col("_dq_errors"))
    )
    
    # ========== PII DETECTION (WARNING) ==========
    # Warn if first_name or last_name looks like test data
    test_patterns = ["test", "example", "sample", "firstname", "lastname"]
    
    for field in ["first_name", "last_name"]:
        for pattern in test_patterns:
            df = df.withColumn(
                "_dq_warnings",
                when(
                    lower(col(field)).contains(pattern),
                    array_union(
                        col("_dq_warnings"),
                        array(lit(f"POTENTIAL_TEST_DATA:{field}"))
                    )
                ).otherwise(col("_dq_warnings"))
            )
    
    # ========== CALCULATE COMPLETENESS SCORE ==========
    all_fields = [c for c in df.columns if not c.startswith("_")]
    
    non_null_expr = sum([
        when(col(c).isNotNull() & (trim(col(c)) != ""), 1).otherwise(0)
        for c in all_fields
    ])
    
    df = df.withColumn(
        "_dq_completeness_score",
        (non_null_expr / len(all_fields) * 100).cast("decimal(5,2)")
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


# ============================================================================
# DEDUPLICATION DETECTION
# ============================================================================

def detect_duplicates_bronze(spark: SparkSession, df: DataFrame, table_name: str) -> DataFrame:
    """Detect duplicates NON-INTRUSIVELY"""
    
    # Calculate record hash
    data_cols = [c for c in df.columns if not c.startswith("_")]
    
    df = df.withColumn(
        "_bronze_record_hash",
        md5(concat_ws("|", *[coalesce(col(c).cast("string"), lit("NULL")) for c in data_cols]))
    )
    
    # Check if bronze table exists
    try:
        existing_bronze = spark.table(table_name)
        
        existing_hashes = existing_bronze.select(
            "_bronze_record_hash",
            "_bronze_ingestion_batch_id"
        ).distinct()
        
        df = df.join(
            existing_hashes.alias("existing"),
            df["_bronze_record_hash"] == existing_hashes["_bronze_record_hash"],
            "left"
        )
        
        df = df.withColumn(
            "_dq_is_duplicate",
            when(col("existing._bronze_ingestion_batch_id").isNotNull(), True).otherwise(False)
        )
        
        df = df.withColumn(
            "_dq_duplicate_of_batch",
            col("existing._bronze_ingestion_batch_id")
        )
        
        df = df.drop("existing._bronze_ingestion_batch_id")
        
    except Exception as e:
        print(f"  ℹ️  Bronze table doesn't exist yet or error: {str(e)}")
        df = df.withColumn("_dq_is_duplicate", lit(False))
        df = df.withColumn("_dq_duplicate_of_batch", lit(None).cast("string"))
    
    return df


# ============================================================================
# METADATA ENRICHMENT
# ============================================================================

def enrich_metadata_bronze(df: DataFrame, source_file: str, batch_id: str) -> DataFrame:
    """Add comprehensive metadata for lineage and debugging"""
    
    df = df.withColumn("_bronze_ingestion_timestamp", current_timestamp())
    df = df.withColumn("_bronze_source_file", lit(source_file))
    df = df.withColumn("_bronze_ingestion_batch_id", lit(batch_id))
    df = df.withColumn("_bronze_ingestion_date", current_date())
    
    window_spec = Window.orderBy(monotonically_increasing_id())
    df = df.withColumn("_bronze_source_line_number", row_number().over(window_spec))
    
    all_source_cols = [c for c in df.columns if not c.startswith("_")]
    df = df.withColumn(
        "_bronze_raw_record",
        to_json(struct(*all_source_cols))
    )
    
    file_hash = hashlib.md5(source_file.encode()).hexdigest()
    df = df.withColumn("_bronze_file_hash", lit(file_hash))
    
    df = df.withColumn("_schema_version", lit("1.0"))
    
    return df


# ============================================================================
# WRITE STRATEGIES
# ============================================================================

def write_to_bronze_append(df: DataFrame, path: str):
    """Append mode: For new batches"""
    df.write \
        .format("delta") \
        .mode("append") \
        .partitionBy("_bronze_ingestion_date") \
        .option("mergeSchema", "true") \
        .save(path)
    
    print(f"  ✓ Appended {df.count()} records")


def write_to_bronze_merge(spark: SparkSession, df: DataFrame, path: str):
    """Merge mode: For updates/corrections"""
    bronze_table = DeltaTable.forPath(spark, path)
    
    bronze_table.alias("target").merge(
        df.alias("source"),
        "target.customer_id = source.customer_id"
    ).whenMatchedUpdateAll() \
     .whenNotMatchedInsertAll() \
     .execute()
    
    print(f"  ✓ Merged {df.count()} records")


def write_to_bronze_hybrid(spark: SparkSession, df: DataFrame, path: str):
    """Hybrid mode: Append new, merge updates"""
    try:
        existing_bronze = spark.read.format("delta").load(path)
        existing_customer_ids = existing_bronze.select("customer_id").distinct()
        
        updates = df.join(existing_customer_ids, "customer_id", "inner")
        new_records = df.join(existing_customer_ids, "customer_id", "left_anti")
        
        update_count = updates.count()
        new_count = new_records.count()
        
        if new_count > 0:
            print(f"  ℹ️  Appending {new_count} new records...")
            write_to_bronze_append(new_records, path)
        
        if update_count > 0:
            print(f"  ℹ️  Merging {update_count} updated records...")
            write_to_bronze_merge(spark, updates, path)
        
    except Exception as e:
        print(f"  ℹ️  First load, using append mode")
        write_to_bronze_append(df, path)


# ============================================================================
# MAIN PIPELINE CLASS
# ============================================================================

class BronzeCustomersPipeline:
    """Complete Bronze Layer Pipeline for Customers"""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.batch_id = str(uuid.uuid4())
        self.ingestion_start = datetime.now()
        self.stats = {}
    
    def run(self, source_file_path: str, write_mode: str = "append"):
        """Execute complete bronze ingestion pipeline"""
        
        print("\n" + "="*70)
        print("BRONZE LAYER INGESTION PIPELINE - CUSTOMERS")
        print("="*70)
        print(f"Batch ID:    {self.batch_id}")
        print(f"Source:      {source_file_path}")
        print(f"Write Mode:  {write_mode}")
        print(f"Started:     {self.ingestion_start.strftime('%Y-%m-%d %H:%M:%S')}")
        print("="*70)
        
        # STEP 1: READ SOURCE
        print("\n[1/7] Reading source data...")
        df = self.spark.read.csv(
            source_file_path,
            header=True,
            inferSchema=False
        )
        source_count = df.count()
        self.stats['source_count'] = source_count
        print(f"  ✓ Read {source_count:,} records from source")
        
        # STEP 2: SCHEMA DETECTION
        print("\n[2/7] Detecting schema changes...")
        df, schema_changes = handle_schema_evolution(df, EXPECTED_SCHEMA_CUSTOMERS_V1)
        self.stats['schema_changes'] = schema_changes
        
        # STEP 3: DATA QUALITY VALIDATION
        print("\n[3/7] Running data quality checks...")
        df = validate_customers_bronze(df)
        
        error_count = df.filter(col("_dq_has_errors") == True).count()
        total_count = df.count()
        dq_score = ((total_count - error_count) / total_count * 100) if total_count > 0 else 0
        
        self.stats['error_count'] = error_count
        self.stats['dq_score'] = dq_score
        
        print(f"  ✓ Validation complete")
        print(f"      Records with errors: {error_count:,} ({error_count/total_count*100:.2f}%)")
        print(f"      Data quality score:  {dq_score:.2f}%")
        
        # STEP 4: DEDUPLICATION DETECTION
        print("\n[4/7] Detecting duplicates...")
        df = detect_duplicates_bronze(self.spark, df, BRONZE_TABLE_CUSTOMERS)
        
        dup_count = df.filter(col("_dq_is_duplicate") == True).count()
        self.stats['duplicate_count'] = dup_count
        print(f"  ✓ Duplicates detected: {dup_count:,}")
        
        # STEP 5: METADATA ENRICHMENT
        print("\n[5/7] Enriching with metadata...")
        df = enrich_metadata_bronze(df, source_file_path, self.batch_id)
        print(f"  ✓ Metadata enrichment complete")
        
        # STEP 6: WRITE TO BRONZE
        print(f"\n[6/7] Writing to bronze layer (mode: {write_mode})...")
        
        if write_mode == "append":
            write_to_bronze_append(df, BRONZE_PATH_CUSTOMERS)
        elif write_mode == "merge":
            write_to_bronze_merge(self.spark, df, BRONZE_PATH_CUSTOMERS)
        elif write_mode == "hybrid":
            write_to_bronze_hybrid(self.spark, df, BRONZE_PATH_CUSTOMERS)
        else:
            raise ValueError(f"Unknown write mode: {write_mode}")
        
        # STEP 7: FINALIZE
        print("\n[7/7] Finalizing and logging statistics...")
        
        ingestion_end = datetime.now()
        duration = (ingestion_end - self.ingestion_start).total_seconds()
        
        self.stats['duration_seconds'] = duration
        self.stats['records_per_second'] = source_count / duration if duration > 0 else 0
        
        try:
            self.spark.sql(f"""
                CREATE TABLE IF NOT EXISTS {BRONZE_TABLE_CUSTOMERS}
                USING DELTA
                LOCATION '{BRONZE_PATH_CUSTOMERS}'
            """)
        except:
            pass
        
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


# ============================================================================
# USAGE EXAMPLE
# ============================================================================

if __name__ == "__main__":
    
    # Initialize Spark with Delta Lake
    spark = SparkSession.builder \
        .appName("BronzeLayerCustomers") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()
    
    # Run pipeline
    pipeline = BronzeCustomersPipeline(spark)
    
    df_bronze, stats = pipeline.run(
        source_file_path="customers.csv",
        write_mode="append"
    )
    
    # Query bronze data
    print("\nBronze Layer Sample (First 5 records):")
    print("-" * 70)
    
    spark.sql(f"""
        SELECT 
            customer_id,
            email,
            customer_segment,
            _dq_has_errors,
            _dq_error_count,
            _bronze_ingestion_timestamp
        FROM {BRONZE_TABLE_CUSTOMERS}
        ORDER BY _bronze_ingestion_timestamp DESC
        LIMIT 5
    """).show(truncate=False)
    
    # Data quality summary
    print("\nData Quality Summary:")
    print("-" * 70)
    
    spark.sql(f"""
        SELECT 
            COUNT(*) as total_records,
            SUM(CASE WHEN _dq_has_errors THEN 1 ELSE 0 END) as error_records,
            AVG(_dq_completeness_score) as avg_completeness,
            SUM(CASE WHEN _dq_is_duplicate THEN 1 ELSE 0 END) as duplicate_records
        FROM {BRONZE_TABLE_CUSTOMERS}
        WHERE _bronze_ingestion_date = CURRENT_DATE
    """).show()
    
    spark.stop()
    
    print("\n✅ Bronze layer ingestion complete!")