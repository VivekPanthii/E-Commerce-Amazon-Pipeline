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

BRONZE_ORDER_PATH="./data/bronze/bronze_orders_raw"

SILVER_ORDER_PATH="./data/silver/silver_orders"


spark=get_spark_session()

# Data Quality Filtering
def filter_invalid_records(df:DataFrame)-> DataFrame:
    
    # total_before=df.count()

    df_filtered=df.filter(
        (col("order_id").isNotNull() & (trim(col("order_id"))!="")) &
        (col("customer_id").isNotNull() & (trim(col("customer_id"))!="")) &
        ((col("_dq_has_errors")==False) | (col("_dq_error_count") ==0))
    )

    # total_after=df.count()

    # filtered_count=total_before-total_after
    print(f"   ✓ Filtered out invalid records")
    return df_filtered



def standarized_date(df):
    print("   ✓ Standardizing dates...")

    formats = [
        "yyyy-MM-dd HH:mm:ss",
        "dd-MM-yyyy HH:mm:ss",
        "dd/MM/yyyy HH:mm",
        "MM/dd/yyyy HH:mm",
        "MM/dd/yyyy HH:mm:ss",
        "MM/dd/yyyy"
    ]

    def parse(colname):
        exprs = [expr(f"try_to_timestamp({colname}, '{fmt}')") for fmt in formats]
        return coalesce(*exprs)

    df = df.withColumn("order_date", parse("order_date"))
    df = df.withColumn("created_at", parse("created_at"))
    return df


# standarize numeric 
def standardize_numeric_data(df:DataFrame)->DataFrame:
    fields=["subtotal","tax_amount","discount_amount","total_amount"]
    print("   ✓ Standardizing numeric fields...")

    for field in fields:
        df=df.withColumn(
            field,
            when(
                (col(field).isNotNull() & (col(field).cast("decimal(10,2)") >=0)),
                expr(f"try_cast({field} AS decimal(10,2))")
            ).otherwise(0.0)
        )


    return df

def validate_amount_consistency(df:DataFrame)->DataFrame:
        print("  ✓ Standardizing monetary amounts...")
        df=df.withColumn(
                "calculated_amount",
                (col("subtotal")+
                col("tax_amount")-
                col("discount_amount")).cast("decimal(10,2)")
            )

        df=df.withColumn(
            "amount_consistency",
            abs(col("total_amount")-col("calculated_amount")) <=0.01
        )

        df=df.withColumn(
            "total_amount",
            when(
                ~col("amount_consistency"),
                col("calculated_amount")
            ).otherwise(col("total_amount"))
        ).drop("amount_consistency","calculated_amount")
        return df

def standardize_payment_method(df: DataFrame) -> DataFrame:
    print("   ✓ Standardizing payment methods...")
    
    df = df.withColumn(
        "payment_method",
        lower(trim(col("payment_method")))
    )
    
    df = df.withColumn(
        "payment_method",
        when(col("payment_method").isin(['credit_card', 'credit card', 'creditcard', 'cc']), 'credit_card')
        .when(col("payment_method").isin(['debit_card', 'debit card', 'debitcard']), 'debit_card')
        .when(col("payment_method").isin(['paypal', 'pay pal']), 'paypal')
        .when(col("payment_method").isin(['gift_card', 'gift card', 'giftcard']), 'gift_card')
        .when(col("payment_method").isin(['cod', 'cash on delivery', 'cash']), 'cash_on_delivery')
        .otherwise('other')
    )
    
    return df

def deduplicate_orders(df:DataFrame)-> DataFrame:
    print("   ✓ Deduplicating records...")
    window_spec = Window.partitionBy("order_id").orderBy(col("created_at").desc())
    df_dedup = df.withColumn("row_num", row_number().over(window_spec)) \
                .filter(col("row_num") == 1) \
                .drop("row_num")

    print(f"  ✓ Removed duplicate records")
    
    return df_dedup

def enrich_order_data(df:DataFrame)->DataFrame:
    print("   ✓ Enrichment order data")

    df = df.select(
        "*",
        year(col("order_date")).alias("order_year"),
        month(col("order_date")).alias("order_month"),
        dayofmonth(col("order_date")).alias("order_day"),
        quarter(col("order_date")).alias("order_quarter"),
        dayofweek(col("order_date")).alias("order_day_of_week"),
        weekofyear(col("order_date")).alias("order_week_of_year"),
        hour(col("order_date")).alias("order_hour")
    )


    df=df.withColumn(
        "order_day_name",
        when(col("order_day_of_week") == 1, "Sunday")
        .when(col("order_day_of_week") == 2, "Monday")
        .when(col("order_day_of_week") == 3, "Tuesday")
        .when(col("order_day_of_week") == 4, "Wednesday")
        .when(col("order_day_of_week") == 5, "Thursday")
        .when(col("order_day_of_week") == 6, "Friday")
        .when(col("order_day_of_week") == 7, "Saturday")
    ).withColumn(
        "order_time_period",
        when((col("order_hour") >= 6) & (col("order_hour") < 12), "Morning")
        .when((col("order_hour") >= 12) & (col("order_hour") < 17), "Afternoon")
        .when((col("order_hour") >= 17) & (col("order_hour") < 21), "Evening")
        .otherwise("Night")
    ).withColumn(
        "is_weekend",
        col("order_day_of_week").isin([1,7])
    ).withColumn(
        "order_value_category",
        when(col("total_amount")>=500,"high_value")
        .when(col("total_amount")>=100,"medium_value")
        .otherwise("low_value")
    ).withColumn(
        "discount_percentage",
        when(
            col("subtotal") > 0,
            (col("discount_amount") / col("subtotal") * 100).cast("decimal(10,2)")
        ).otherwise(0.0)
    ).withColumn(
        "has_discount",
        col("discount_amount") > 0
    ).withColumn(
        "days_since_order",
        datediff(current_date(), col("order_date").cast("date"))
    )
    return df

def select_final_columns(df:DataFrame)->DataFrame:
    return df.select(
            "order_id",
            "customer_id",
            "order_date",
            "subtotal",
            "tax_amount",
            "discount_amount",
            "total_amount",
            "payment_method",
            "created_at",
            "order_year",
            "order_month",
            "order_day",
            "order_quarter",
            "order_day_of_week",
            "order_day_name",
            "order_week_of_year",
            "order_hour",
            "order_time_period",
            "is_weekend"


        )



class SilverPipeline:
    def __init__(self,metadata_manager=None):
        self.batch_id=str(uuid.uuid4())
        self.processing_start=datetime.now()
        self.stats={}
        self.metadata_manager=metadata_manager

    def run(self,bronze_batch_id: str=None):
        print("\n" + "="*70)
        print("SILVER LAYER TRANSFORMATION PIPELINE - ORDERS")
        print("="*70)
        print(f"Batch ID:    {self.batch_id}")
        print(f"Started:     {self.processing_start.strftime('%Y-%m-%d %H:%M:%S')}")
        print("="*70)

        print("\n[1/9] Reading from bronze layer...")
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
                    print("    ✓ No existing silver data, processing all bronze records")

                records_input=bronze_df.count()
                self.stats["records_input"]=records_input

                print(f"    ✓ Read {records_input:,} record from bronze")

                if records_input==0:
                    print("\n No new records to process")
                    return None, self.stats
        except Exception as e:
            print(f"    Error reading bronze: {e}")
            raise

        source_batch_id=bronze_df.select("_bronze_ingestion_batch_id").first()[0]

        df = bronze_df.drop(*[c for c in bronze_df.columns if c.startswith("_")])

        print("\n[2/9] Standardizing data...")
        # persist (cache) once — we'll reuse df across multiple transformations and avoid re-computation
        df = df.persist(StorageLevel.MEMORY_AND_DISK)
        df=standarized_date(df)
        df=standardize_numeric_data(df)
        df=standardize_payment_method(df)
        print("    ✓ Stantardization complete")

        print("\n[3/9] Validating amount consistency...")
        df=validate_amount_consistency(df)
        print("   ✓ Amount validation complete")
        
        print("\n[4/9] Filtering invalid records...")
        # records_before_filter=df.count()
        df=filter_invalid_records(df)
        # records_after_filter=df.count()

        # self.stats["records_filtered"]=records_before_filter-records_after_filter

        print("   ✓ Filtering complete")

        print("\n[5/9] Deduplicating records...")

        # records_before_dedup=df.count()
        df=deduplicate_orders(df) 
        # records_after_dedup=df.count()
        # self.stats['records_deduplicated'] = records_before_dedup - records_after_dedup
        print("  ✓ Deduplication complete")


        print("\n[6/9] Enriching order data...")
        df=enrich_order_data(df)
        print("   ✓ Enrichment complete")

        print("\n[7/9] Adding silver metadata...")
        
        df = add_silver_metadata(df, self.batch_id, source_batch_id)
        
        print("  ✓ Silver metadata added")

        print("\n[8/9] Selecting final columns...")
        
        df_final = select_final_columns(df)
        records_output = df_final.count()
        
        self.stats['records_output'] = records_output
        
        print("  ✓ Final columns selected")

        print("\n[9/9] Writing to silver layer...")
        df_final.write \
            .format("delta") \
            .mode("append") \
            .partitionBy("order_year", "order_month") \
            .option("mergeSchema", "true") \
            .save(SILVER_ORDER_PATH)
        
        df.unpersist()
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
