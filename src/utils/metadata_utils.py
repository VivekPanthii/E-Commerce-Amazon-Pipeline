"""
Metadata Management System - Separate Tables Implementation
Complete production-ready code for tracking pipeline execution metadata
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime
import uuid
import json
from typing import Dict, Any, Optional

# ============================================================================
# METADATA TABLE SCHEMAS
# ============================================================================

# Bronze Layer Batch Metadata
BRONZE_BATCH_LOG_SCHEMA = StructType([
    StructField("batch_id", StringType(), False),
    StructField("table_name", StringType(), False),
    StructField("layer", StringType(), False),
    StructField("source_file_path", StringType(), True),
    StructField("ingestion_timestamp", TimestampType(), False),
    StructField("ingestion_date", DateType(), False),
    StructField("records_total", LongType(), False),
    StructField("records_with_errors", LongType(), False),
    StructField("records_duplicate", LongType(), False),
    StructField("data_quality_score", DecimalType(5, 2), True),
    StructField("processing_duration_seconds", DecimalType(10, 2), False),
    StructField("schema_version", StringType(), True),
    StructField("schema_changes_detected", BooleanType(), False),
    StructField("schema_changes_detail", StringType(), True),  # JSON
    StructField("file_size_mb", DecimalType(12, 2), True),
    StructField("write_mode", StringType(), True),
    StructField("status", StringType(), False),  # SUCCESS, FAILED, RUNNING
    StructField("error_message", StringType(), True),
    StructField("pipeline_version", StringType(), True),
    StructField("executed_by", StringType(), True),
    StructField("created_at", TimestampType(), False)
])

# Silver Layer Batch Metadata
SILVER_BATCH_LOG_SCHEMA = StructType([
    StructField("batch_id", StringType(), False),
    StructField("source_batch_id", StringType(), True),  # Link to bronze
    StructField("table_name", StringType(), False),
    StructField("layer", StringType(), False),
    StructField("processing_timestamp", TimestampType(), False),
    StructField("processing_date", DateType(), False),
    StructField("records_input", LongType(), False),
    StructField("records_output", LongType(), False),
    StructField("records_filtered", LongType(), False),
    StructField("records_deduplicated", LongType(), False),
    StructField("records_standardized", LongType(), False),
    StructField("transformation_rules_applied", StringType(), True),  # JSON array
    StructField("processing_duration_seconds", DecimalType(10, 2), False),
    StructField("data_quality_score", DecimalType(5, 2), True),
    StructField("status", StringType(), False),
    StructField("error_message", StringType(), True),
    StructField("pipeline_version", StringType(), True),
    StructField("executed_by", StringType(), True),
    StructField("created_at", TimestampType(), False)
])

# Gold Layer Batch Metadata
GOLD_BATCH_LOG_SCHEMA = StructType([
    StructField("batch_id", StringType(), False),
    StructField("source_batch_id", StringType(), True),  # Link to silver
    StructField("table_name", StringType(), False),
    StructField("table_type", StringType(), False),  # DIMENSION, FACT
    StructField("layer", StringType(), False),
    StructField("processing_timestamp", TimestampType(), False),
    StructField("processing_date", DateType(), False),
    StructField("records_inserted", LongType(), False),
    StructField("records_updated", LongType(), False),
    StructField("records_deleted", LongType(), False),
    StructField("scd_type2_updates", LongType(), True),  # For dimensions
    StructField("aggregation_level", StringType(), True),  # daily, monthly, etc.
    StructField("business_rules_applied", StringType(), True),  # JSON
    StructField("processing_duration_seconds", DecimalType(10, 2), False),
    StructField("status", StringType(), False),
    StructField("error_message", StringType(), True),
    StructField("pipeline_version", StringType(), True),
    StructField("executed_by", StringType(), True),
    StructField("created_at", TimestampType(), False)
])

# Data Quality Issues Detail
DATA_QUALITY_ISSUES_SCHEMA = StructType([
    StructField("issue_id", LongType(), False),
    StructField("batch_id", StringType(), False),
    StructField("layer", StringType(), False),
    StructField("table_name", StringType(), False),
    StructField("record_identifier", StringType(), True),  # order_id, customer_id, etc.
    StructField("error_type", StringType(), False),
    StructField("error_category", StringType(), False),  # MISSING, INVALID, RANGE, etc.
    StructField("error_field", StringType(), False),
    StructField("error_message", StringType(), True),
    StructField("error_value", StringType(), True),
    StructField("severity", StringType(), False),  # ERROR, WARNING
    StructField("detected_at", TimestampType(), False),
    StructField("created_at", TimestampType(), False)
])

# Schema Evolution History
SCHEMA_HISTORY_SCHEMA = StructType([
    StructField("schema_id", LongType(), False),
    StructField("table_name", StringType(), False),
    StructField("layer", StringType(), False),
    StructField("schema_version", StringType(), False),
    StructField("change_type", StringType(), False),  # COLUMN_ADDED, COLUMN_REMOVED, TYPE_CHANGED
    StructField("change_detail", StringType(), True),  # JSON
    StructField("detected_at", TimestampType(), False),
    StructField("batch_id", StringType(), True),
    StructField("created_at", TimestampType(), False)
])

# Pipeline Execution Log (Cross-layer tracking)
PIPELINE_EXECUTION_SCHEMA = StructType([
    StructField("execution_id", StringType(), False),
    StructField("pipeline_name", StringType(), False),
    StructField("execution_date", DateType(), False),
    StructField("start_timestamp", TimestampType(), False),
    StructField("end_timestamp", TimestampType(), True),
    StructField("duration_seconds", DecimalType(10, 2), True),
    StructField("bronze_batch_id", StringType(), True),
    StructField("silver_batch_id", StringType(), True),
    StructField("gold_batch_id", StringType(), True),
    StructField("status", StringType(), False),  # RUNNING, SUCCESS, FAILED, PARTIAL
    StructField("records_processed", LongType(), True),
    StructField("error_message", StringType(), True),
    StructField("triggered_by", StringType(), True),  # AIRFLOW, MANUAL, SCHEDULED
    StructField("created_at", TimestampType(), False)
])


# ============================================================================
# METADATA MANAGER CLASS
# ============================================================================

class MetadataManager:
    """
    Centralized metadata management for all pipeline layers
    """
    
    def __init__(self, spark: SparkSession, metadata_path: str = "/tmp/metadata"):
        self.spark = spark
        self.metadata_path = metadata_path
        self._initialize_tables()
    
    def _initialize_tables(self):
        """Create metadata tables if they don't exist"""
        
        tables = {
            "bronze_batch_log": BRONZE_BATCH_LOG_SCHEMA,
            "silver_batch_log": SILVER_BATCH_LOG_SCHEMA,
            "gold_batch_log": GOLD_BATCH_LOG_SCHEMA,
            "data_quality_issues": DATA_QUALITY_ISSUES_SCHEMA,
            "schema_history": SCHEMA_HISTORY_SCHEMA,
            "pipeline_execution": PIPELINE_EXECUTION_SCHEMA
        }
        
        for table_name, schema in tables.items():
            table_path = f"{self.metadata_path}/{table_name}"
            
            # Create empty table if doesn't exist
            try:
                self.spark.read.format("delta").load(table_path)
                print(f"  ✓ Table exists: metadata.{table_name}")
            except:
                empty_df = self.spark.createDataFrame([], schema)
                empty_df.write.format("delta").mode("overwrite").save(table_path)
                
                # Create table reference
                self.spark.sql(f"""
                    CREATE TABLE IF NOT EXISTS metadata_{table_name}
                    USING DELTA
                    LOCATION '{table_path}'
                """)
                print(f"  ✓ Created: metadata.{table_name}")
    
    # ========================================================================
    # BRONZE LAYER METADATA
    # ========================================================================
    
    def log_bronze_batch(
        self,
        batch_id: str,
        table_name: str,
        source_file_path: str,
        records_total: int,
        records_with_errors: int,
        records_duplicate: int,
        data_quality_score: float,
        processing_duration: float,
        schema_version: str,
        schema_changes: Dict[str, Any],
        write_mode: str,
        status: str = "SUCCESS",
        error_message: Optional[str] = None,
        file_size_mb: Optional[float] = None
    ):
        """
        Log bronze layer batch execution metadata
        """
        
        batch_data = {
            "batch_id": batch_id,
            "table_name": table_name,
            "layer": "bronze",
            "source_file_path": source_file_path,
            "ingestion_timestamp": datetime.now(),
            "ingestion_date": datetime.now().date(),
            "records_total": records_total,
            "records_with_errors": records_with_errors,
            "records_duplicate": records_duplicate,
            "data_quality_score": data_quality_score,
            "processing_duration_seconds": processing_duration,
            "schema_version": schema_version,
            "schema_changes_detected": schema_changes.get("has_changes", False),
            "schema_changes_detail": json.dumps(schema_changes) if schema_changes else None,
            "file_size_mb": file_size_mb,
            "write_mode": write_mode,
            "status": status,
            "error_message": error_message,
            "pipeline_version": "1.0",
            "executed_by": "airflow",  # Could be parameterized
            "created_at": datetime.now()
        }
        
        df = self.spark.createDataFrame([batch_data], BRONZE_BATCH_LOG_SCHEMA)
        
        df.write.format("delta").mode("append").save(f"{self.metadata_path}/bronze_batch_log")
        
        print(f"  ✓ Bronze metadata logged: {batch_id}")
    
    # ========================================================================
    # SILVER LAYER METADATA
    # ========================================================================
    
    def log_silver_batch(
        self,
        batch_id: str,
        source_batch_id: str,
        table_name: str,
        records_input: int,
        records_output: int,
        records_filtered: int,
        records_deduplicated: int,
        records_standardized: int,
        transformation_rules: list,
        processing_duration: float,
        data_quality_score: float,
        status: str = "SUCCESS",
        error_message: Optional[str] = None
    ):
        """
        Log silver layer batch execution metadata
        """
        
        batch_data = {
            "batch_id": batch_id,
            "source_batch_id": source_batch_id,
            "table_name": table_name,
            "layer": "silver",
            "processing_timestamp": datetime.now(),
            "processing_date": datetime.now().date(),
            "records_input": records_input,
            "records_output": records_output,
            "records_filtered": records_filtered,
            "records_deduplicated": records_deduplicated,
            "records_standardized": records_standardized,
            "transformation_rules_applied": json.dumps(transformation_rules),
            "processing_duration_seconds": processing_duration,
            "data_quality_score": data_quality_score,
            "status": status,
            "error_message": error_message,
            "pipeline_version": "1.0",
            "executed_by": "airflow",
            "created_at": datetime.now()
        }
        
        df = self.spark.createDataFrame([batch_data], SILVER_BATCH_LOG_SCHEMA)
        
        df.write.format("delta").mode("append").save(f"{self.metadata_path}/silver_batch_log")
        
        print(f"  ✓ Silver metadata logged: {batch_id}")
    
    # ========================================================================
    # GOLD LAYER METADATA
    # ========================================================================
    
    def log_gold_batch(
        self,
        batch_id: str,
        source_batch_id: str,
        table_name: str,
        table_type: str,
        records_inserted: int,
        records_updated: int,
        records_deleted: int,
        scd_type2_updates: Optional[int],
        aggregation_level: Optional[str],
        business_rules: list,
        processing_duration: float,
        status: str = "SUCCESS",
        error_message: Optional[str] = None
    ):
        """
        Log gold layer batch execution metadata
        """
        
        batch_data = {
            "batch_id": batch_id,
            "source_batch_id": source_batch_id,
            "table_name": table_name,
            "table_type": table_type,
            "layer": "gold",
            "processing_timestamp": datetime.now(),
            "processing_date": datetime.now().date(),
            "records_inserted": records_inserted,
            "records_updated": records_updated,
            "records_deleted": records_deleted,
            "scd_type2_updates": scd_type2_updates,
            "aggregation_level": aggregation_level,
            "business_rules_applied": json.dumps(business_rules),
            "processing_duration_seconds": processing_duration,
            "status": status,
            "error_message": error_message,
            "pipeline_version": "1.0",
            "executed_by": "airflow",
            "created_at": datetime.now()
        }
        
        df = self.spark.createDataFrame([batch_data], GOLD_BATCH_LOG_SCHEMA)
        
        df.write.format("delta").mode("append").save(f"{self.metadata_path}/gold_batch_log")
        
        print(f"  ✓ Gold metadata logged: {batch_id}")
    
    # ========================================================================
    # DATA QUALITY ISSUES TRACKING
    # ========================================================================
    
    def log_data_quality_issues(
        self,
        batch_id: str,
        layer: str,
        table_name: str,
        issues_df: DataFrame
    ):
        """
        Log detailed data quality issues
        
        Args:
            issues_df: DataFrame with columns:
                - record_identifier
                - error_type
                - error_field
                - error_message
                - error_value
                - severity
        """
        
        # Add metadata columns
        issues_enriched = issues_df \
            .withColumn("issue_id", monotonically_increasing_id()) \
            .withColumn("batch_id", lit(batch_id)) \
            .withColumn("layer", lit(layer)) \
            .withColumn("table_name", lit(table_name)) \
            .withColumn("detected_at", current_timestamp()) \
            .withColumn("created_at", current_timestamp())
        
        # Parse error_type into category
        issues_enriched = issues_enriched.withColumn(
            "error_category",
            when(col("error_type").startswith("MISSING"), "MISSING")
            .when(col("error_type").startswith("INVALID"), "INVALID")
            .when(col("error_type").startswith("NEGATIVE"), "RANGE")
            .otherwise("OTHER")
        )
        
        # Write to metadata table
        issues_enriched.write.format("delta").mode("append").save(
            f"{self.metadata_path}/data_quality_issues"
        )
        
        issue_count = issues_enriched.count()
        print(f"  ✓ Logged {issue_count} data quality issues")
    
    # ========================================================================
    # SCHEMA EVOLUTION TRACKING
    # ========================================================================
    
    def log_schema_change(
        self,
        table_name: str,
        layer: str,
        schema_version: str,
        change_type: str,
        change_detail: Dict[str, Any],
        batch_id: Optional[str] = None
    ):
        """
        Log schema evolution events
        """
        
        schema_data = {
            "schema_id": int(datetime.now().timestamp() * 1000),  # Millisecond timestamp
            "table_name": table_name,
            "layer": layer,
            "schema_version": schema_version,
            "change_type": change_type,
            "change_detail": json.dumps(change_detail),
            "detected_at": datetime.now(),
            "batch_id": batch_id,
            "created_at": datetime.now()
        }
        
        df = self.spark.createDataFrame([schema_data], SCHEMA_HISTORY_SCHEMA)
        
        df.write.format("delta").mode("append").save(f"{self.metadata_path}/schema_history")
        
        print(f"  ✓ Schema change logged: {change_type}")
    
    # ========================================================================
    # PIPELINE EXECUTION TRACKING
    # ========================================================================
    
    def start_pipeline_execution(
        self,
        execution_id: str,
        pipeline_name: str,
        triggered_by: str = "AIRFLOW"
    ) -> str:
        """
        Start tracking end-to-end pipeline execution
        Returns execution_id
        """
        
        execution_data = {
            "execution_id": execution_id,
            "pipeline_name": pipeline_name,
            "execution_date": datetime.now().date(),
            "start_timestamp": datetime.now(),
            "end_timestamp": None,
            "duration_seconds": None,
            "bronze_batch_id": None,
            "silver_batch_id": None,
            "gold_batch_id": None,
            "status": "RUNNING",
            "records_processed": None,
            "error_message": None,
            "triggered_by": triggered_by,
            "created_at": datetime.now()
        }
        
        df = self.spark.createDataFrame([execution_data], PIPELINE_EXECUTION_SCHEMA)
        
        df.write.format("delta").mode("append").save(f"{self.metadata_path}/pipeline_execution")
        
        print(f"  ✓ Pipeline execution started: {execution_id}")
        return execution_id
    
    def complete_pipeline_execution(
        self,
        execution_id: str,
        bronze_batch_id: Optional[str],
        silver_batch_id: Optional[str],
        gold_batch_id: Optional[str],
        records_processed: int,
        status: str = "SUCCESS",
        error_message: Optional[str] = None
    ):
        """
        Complete pipeline execution tracking
        """
        
        from delta.tables import DeltaTable
        
        pipeline_table = DeltaTable.forPath(
            self.spark, 
            f"{self.metadata_path}/pipeline_execution"
        )
        
        # Get start timestamp
        start_time = self.spark.read.format("delta").load(
            f"{self.metadata_path}/pipeline_execution"
        ).filter(col("execution_id") == execution_id).select("start_timestamp").first()[0]
        
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        # Update record
        pipeline_table.update(
            condition = col("execution_id") == execution_id,
            set = {
                "end_timestamp": lit(end_time),
                "duration_seconds": lit(duration),
                "bronze_batch_id": lit(bronze_batch_id),
                "silver_batch_id": lit(silver_batch_id),
                "gold_batch_id": lit(gold_batch_id),
                "status": lit(status),
                "records_processed": lit(records_processed),
                "error_message": lit(error_message)
            }
        )
        
        print(f"  ✓ Pipeline execution completed: {execution_id} ({status})")
    
    # ========================================================================
    # QUERY METHODS
    # ========================================================================
    
    def get_batch_summary(self, batch_id: str) -> Dict[str, Any]:
        """
        Get complete summary for a batch across all layers
        """
        
        summary = {}
        
        # Bronze info
        bronze = self.spark.read.format("delta").load(
            f"{self.metadata_path}/bronze_batch_log"
        ).filter(col("batch_id") == batch_id).first()
        
        if bronze:
            summary["bronze"] = bronze.asDict()
        
        # Silver info
        silver = self.spark.read.format("delta").load(
            f"{self.metadata_path}/silver_batch_log"
        ).filter(col("source_batch_id") == batch_id).first()
        
        if silver:
            summary["silver"] = silver.asDict()
        
        # Gold info
        gold = self.spark.read.format("delta").load(
            f"{self.metadata_path}/gold_batch_log"
        ).filter(col("source_batch_id") == batch_id).first()
        
        if gold:
            summary["gold"] = gold.asDict()
        
        return summary
    
    def get_data_quality_trend(self, table_name: str, days: int = 7) -> DataFrame:
        """
        Get data quality trend for a table
        """
        
        return self.spark.sql(f"""
            SELECT 
                ingestion_date,
                records_total,
                records_with_errors,
                data_quality_score,
                processing_duration_seconds
            FROM metadata_bronze_batch_log
            WHERE table_name = '{table_name}'
              AND ingestion_date >= CURRENT_DATE - INTERVAL '{days}' DAY
            ORDER BY ingestion_date DESC
        """)
    
    def get_pipeline_performance(self, pipeline_name: str, days: int = 30) -> DataFrame:
        """
        Get pipeline performance metrics
        """
        
        return self.spark.sql(f"""
            SELECT 
                execution_date,
                status,
                duration_seconds,
                records_processed,
                triggered_by
            FROM metadata_pipeline_execution
            WHERE pipeline_name = '{pipeline_name}'
              AND execution_date >= CURRENT_DATE - INTERVAL '{days}' DAY
            ORDER BY execution_date DESC
        """)


# ============================================================================
# USAGE EXAMPLE
# ============================================================================

if __name__ == "__main__":
    
    # Initialize Spark
    spark = SparkSession.builder \
        .appName("MetadataManagement") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()
    
    # Initialize metadata manager
    metadata_mgr = MetadataManager(spark, metadata_path="/tmp/metadata")
    
    print("\n" + "="*70)
    print("METADATA MANAGEMENT SYSTEM DEMO")
    print("="*70)
    
    # Example: Log bronze batch
    print("\n1. Logging Bronze Batch...")
    batch_id = str(uuid.uuid4())
    
    metadata_mgr.log_bronze_batch(
        batch_id=batch_id,
        table_name="bronze.orders_raw",
        source_file_path="s3://landing/orders/2024-11-16/orders.csv",
        records_total=5000,
        records_with_errors=250,
        records_duplicate=50,
        data_quality_score=95.0,
        processing_duration=45.5,
        schema_version="1.0",
        schema_changes={"has_changes": False},
        write_mode="append",
        file_size_mb=2.5
    )
    
    # Example: Log silver batch
    print("\n2. Logging Silver Batch...")
    silver_batch_id = str(uuid.uuid4())
    
    metadata_mgr.log_silver_batch(
        batch_id=silver_batch_id,
        source_batch_id=batch_id,
        table_name="silver.orders_cleaned",
        records_input=5000,
        records_output=4700,
        records_filtered=250,
        records_deduplicated=50,
        records_standardized=4700,
        transformation_rules=["date_standardization", "amount_validation", "deduplication"],
        processing_duration=32.3,
        data_quality_score=99.5
    )
    
    # Example: Start pipeline execution
    print("\n3. Tracking Pipeline Execution...")
    exec_id = str(uuid.uuid4())
    metadata_mgr.start_pipeline_execution(
        execution_id=exec_id,
        pipeline_name="orders_daily_pipeline",
        triggered_by="AIRFLOW"
    )
    
    # Simulate work...
    import time
    time.sleep(2)
    
    # Complete pipeline
    metadata_mgr.complete_pipeline_execution(
        execution_id=exec_id,
        bronze_batch_id=batch_id,
        silver_batch_id=silver_batch_id,
        gold_batch_id=None,
        records_processed=4700,
        status="SUCCESS"
    )
    
    # Query metadata
    print("\n4. Querying Metadata...")
    print("\nBatch Summary:")
    summary = metadata_mgr.get_batch_summary(batch_id)
    print(json.dumps(summary, indent=2, default=str))
    
    print("\n" + "="*70)
    print("METADATA TABLES CREATED SUCCESSFULLY!")
    print("="*70)
    print("\nAvailable tables:")
    print("  - metadata_bronze_batch_log")
    print("  - metadata_silver_batch_log")
    print("  - metadata_gold_batch_log")
    print("  - metadata_data_quality_issues")
    print("  - metadata_schema_history")
    print("  - metadata_pipeline_execution")
    
    spark.stop()