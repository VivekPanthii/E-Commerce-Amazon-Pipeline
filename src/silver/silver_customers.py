from src.utils.spark_utils import get_spark_session
from src.silver.silver_metadata import add_silver_metadata
from pyspark.sql import DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *
from typing import Dict, List, Tuple
from datetime import datetime
from pyspark.sql import Window
import uuid

BRONZE_ORDER_PATH="./data/bronze/bronze_customers_raw"

SILVER_ORDER_PATH="./data/silver/silver_customers"

spark=get_spark_session()

def filter_invalid_records(df:DataFrame)->DataFrame:
    print("    Filtering invalid records...")

    # total_before=df.count()
    df_filtered=df.filter(
        (col("customer_id").isNotNull()) & (trim(col("customer_id"))!="")
        & (col("email").isNotNull()) & (trim(col("email"))!="") &
        (col("_dq_has_errors")==False) | (col("_dq_error_count")==0)
    )
    # total_after=df_filtered.count()
    # filtered_count=total_before-total_after
    print(f"    Filtered out invalid records")
    return df_filtered

def standard_name(df:DataFrame)->DataFrame:
    fields=["first_name","last_name"]
    for field in fields:
        df=df.withColumn(
            field,
            when(
                col(field).isNotNull() & (trim(col(field))!=""),
                initcap(col(field))
            ).otherwise(lit(None))
        )
    return df

def standardize_email(df:DataFrame)-> DataFrame:
    print("   Standardizing emails...")
    email_pattern=r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'

    df=df.withColumn(
        "email_valid",
        when(
            col("email").rlike(email_pattern), True
        ).otherwise(False)
    )
    return df

def standardize_date(df: DataFrame)->DataFrame:
    print("  → Standardizing dates...")

    df=df.withColumn(
        "registration_date_cleaned",
        coalesce(
            to_date(col("registration_date"), "yyyy-MM-dd"),
            to_date(col("registration_date"), "MM/dd/yyyy"),
            to_date(col("registration_date"), "dd-MM-yyyy"),
            to_date(col("registration_date"), "yyyy-MM-dd'T'HH:mm:ss")
        )
    )
    df = df.withColumn(
        "registration_date_parsed",
        when(col("registration_date_cleaned").isNull() & 
             col("registration_date").isNotNull(), False).otherwise(True)
    )
    
    return df

def standardize_segment(df:DataFrame)->DataFrame:
    print("   Standardizing customer segments...")

    df=df.withColumn(
        "customer_segment_cleaned",
        when(
            lower(trim(col("customer_segment"))).isin(["premium","standard","basic","vip"]),
            lower(trim(col("customer_segment")))
        ).otherwise("standard")
    )
    return df

def standardize_numeric_fields(df: DataFrame) -> DataFrame:

    print("   Standardizing numeric fields...")
    
    df = df.withColumn(
        "lifetime_value_cleaned",
        when(
            col("lifetime_value").isNotNull(),
            col("lifetime_value").cast("decimal(12,2)")
        ).otherwise(lit(0.0).cast("decimal(12,2)"))
    )
    

    df = df.withColumn(
        "lifetime_value_cleaned",
        when(
            col("lifetime_value_cleaned") < 0,
            lit(0.0).cast("decimal(12,2)")
        ).otherwise(col("lifetime_value_cleaned"))
    )
    
    return df


def standardize_boolean_fields(df: DataFrame) -> DataFrame:

    print("   Standardizing boolean fields...")
    
    df = df.withColumn(
        "is_active_cleaned",
        when(
            lower(trim(col("is_active"))).isin(['true', '1', 'y', 'yes']),
            True
        ).when(
            lower(trim(col("is_active"))).isin(['false', '0', 'n', 'no']),
            False
        ).otherwise(None)
    )
    
    return df


def standardize_phone_us_format(df: DataFrame) -> DataFrame:
    print("   Standardizing phone numbers to (XXX) XXX-XXXX format...")

    df = df.withColumn(
        "phone_digits",
        regexp_replace(col("phone"), "[^0-9]", "")
    )

    df = df.withColumn(
        "phone_digits",
        when(length(col("phone_digits")) >= 10, substring(col("phone_digits"), -10, 10))
        .otherwise(None)
    )

    df = df.withColumn(
        "phone",
        when(col("phone_digits").isNotNull(),
             concat(
                 lit("("), substring(col("phone_digits"), 1, 3), lit(") "),
                 substring(col("phone_digits"), 4, 3), lit("-"),
                 substring(col("phone_digits"), 7, 4)
             )
        ).otherwise(None)
    )


    df = df.drop("phone_digits")

    return df


def deduplicate_customers(df:DataFrame)->DataFrame:
    print("   Deduplicating records...")

    window_spec=Window.partitionBy("customer_id").orderBy(col("_bronze_ingestion_timestamp").desc())
    df=df.withColumn(
        "row_num",
        row_number().over(window_spec)
    )
    # total_before=df.count()
    df_duped=df.filter(col("row_num")==1).drop("row_num")
    # total_after=df_duped.count()

    # duplicates_removed=total_before-total_after

    print(f"      Removed duplicate records")
    
    return df_duped


def enrich_customers_data(df:DataFrame)->DataFrame:
    print("   Enriching customer data...")
    df=df.withColumn(
        "full_name",
        when(
            col("first_name").isNotNull() & col("last_name").isNotNull(),
            concat(col("first_name"), lit(" "), col("last_name"))
        ).when(
            col("first_name").isNotNull(),
            col("first_name")
        ).when(
            col("last_name").isNotNull(),
            col("last_name")
        ).otherwise(None)
    )

    df=df.withColumn(
        "customer_age_days",
        when(
            col("registration_date_cleaned").isNotNull(),
            datediff(current_date(),col("registration_date_cleaned"))
        ).otherwise(None)
    )
    df = df.withColumn(
        "customer_tier",
        when(col("lifetime_value_cleaned") >= 5000, "platinum")
        .when(col("lifetime_value_cleaned") >= 2000, "gold")
        .when(col("lifetime_value_cleaned") >= 500, "silver")
        .otherwise("bronze")
    )

    df=df.withColumn(
        "email_domain",
        when(
            col("email").isNotNull(),
            regexp_extract(col("email"),r'@(.+)$',1)
        ).otherwise(None)
    )

    df = df.withColumn(
        "has_verified_contact",
        (col("email_valid") == True) | (col("phone").isNotNull())
    )
    return df



def select_final_columns(df:DataFrame)->DataFrame:
    return df.select(
        "customer_id",
        "first_name",
        "last_name",
        "full_name",
        "email",
        "email_valid",
        "email_domain",
        "phone",
        "has_verified_contact",
        col("registration_date_cleaned").alias("registration_date"),
        "registration_date_parsed",
        "customer_age_days",
        col("customer_segment_cleaned").alias("customer_segment"),
        col("lifetime_value_cleaned").alias("lifetime_value"),
        "customer_tier",
        col("is_active_cleaned").alias("is_active"),
        "_silver_processing_timestamp",
        "_silver_processing_date",
        "_silver_batch_id",
        "_silver_source_batch_id",
        "_silver_record_hash",
    )


    

class SilverPipeline:
    def __init__(self,metadata_manager=None):
        self.batch_id=str(uuid.uuid4())
        self.processing_start=datetime.now()
        self.stats={}
        self.metadata_manager=metadata_manager

    def run(self,bronze_batch_id: str=None):
        print("\n" + "="*70)
        print("SILVER LAYER TRANSFORMATION PIPELINE - CUSTOMERS")
        print("="*70)
        print(f"Batch ID:    {self.batch_id}")
        print(f"Started:     {self.processing_start.strftime('%Y-%m-%d %H:%M:%S')}")
        print("="*70)

        print("\n[1/8] Reading from bronze layer...")
        try:
            bronze_df=spark.read.format("delta").load(BRONZE_ORDER_PATH)
            if bronze_batch_id:
                bronze_df=bronze_df.filter(
                    col("_bronze_ingestion_batch_id") == bronze_batch_id
                )

            else:
                try:
                    silver_df=spark.read.format("delta").load(SILVER_ORDER_PATH)
                    processed_batch=silver_df.select("_bronze_ingestion_batch_id").distinct()


                    bronze_df=bronze_df.join(
                        processed_batch,"_bronze_ingestion_batch_id","left_anti"
                    )
                except:
                    print("     No existing silver data, processing all bronze records")

                records_input=bronze_df.count()
                self.stats["records_input"]=records_input

                print(f"    Read {records_input:,} record from bronze")

                if records_input==0:
                    print("\n No new records to process")
                    return None, self.stats
        except Exception as e:
            print(f"    Error reading bronze: {e}")
            raise

        source_batch_id=bronze_df.select("_bronze_ingestion_batch_id").first()[0]

        print("\n[2/8] Standardizing data...")
        df=bronze_df
        df=standard_name(df)
        df=standardize_email(df)
        df=standardize_date(df)
        df=standardize_segment(df)
        df=standardize_numeric_fields(df)
        df=standardize_boolean_fields(df)
        df=standardize_phone_us_format(df)
        print("     Stantardization complete")

        print("\n[3/8] Deduplicating records...")

        # records_before_dedup=df.count()
        df=deduplicate_customers(df) 
        # records_after_dedup=df.count()
        # self.stats['records_deduplicated'] = records_before_dedup - records_after_dedup
        print("   Deduplication complete")

        print("\n[4/9] Filtering invalid records...")
        # records_before_filter=df.count()
        df=filter_invalid_records(df)
        # records_after_filter=df.count()

        # self.stats["records_filtered"]=records_before_filter-records_after_filter

        print("   Filtering complete")

        print("\n[5/8] Enriching order data...")
        df=enrich_customers_data(df)
        print("   Enrichment complete")

        print("\n[6/8] Adding silver metadata...")
        
        df = add_silver_metadata(df, self.batch_id, source_batch_id)
        
        print("  ✓ Silver metadata added")

        print("\n[7/8] Selecting final columns...")
        
        df_final = select_final_columns(df)
        records_output = df_final.count()
        
        self.stats['records_output'] = records_output
        
        print("  ✓ Final columns selected")

        print("\n[8/8] Writing to silver layer...")
        df_final.write \
            .format("delta") \
            .mode("append") \
            .partitionBy("_silver_processing_date") \
            .option("mergeSchema", "true") \
            .save(SILVER_ORDER_PATH)
        processing_end=datetime.now()
        duration=(processing_end-self.processing_start).total_seconds()
        self.stats['duration_seconds'] = duration
        self.stats['records_per_second'] = records_output / duration if duration > 0 else 0
        dq_improvement = ((records_output / records_input) * 100) if records_input > 0 else 0
        self.stats['data_quality_score'] = dq_improvement
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
    

if __name__ =="__main__":
    pipeline=SilverPipeline()
    df_silver,stats=pipeline.run()