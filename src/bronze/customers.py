from ..utils.spark_utils import get_spark_session
from ..utils.metadata import enriched_metadata
from pyspark.sql import DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *
from typing import Dict, List, Tuple
from datetime import datetime
import uuid
from ..utils.change_detect import handle_schema_changes
from ..utils.duplicates_detect import detect_duplicates_bronze
from ..utils.write_operation import write_to_append
from ..utils.completeness import completeness_final_set
CUSTOMER_PATH=f"./data/landing/customers/"

spark=get_spark_session()
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
        {"name": "is_active", "type": "string", "required": False},
        {"name": "lifetime_value", "type": "string", "required": False}
    ]
}


BRONZE_TABLE_CUSTOMERS="bronze_customers_raw"
BRONZE_ROOT_PATH=f"./data/bronze/bronze_customers_raw"


source_file_path=f"./data/landing/customers/customers.csv"

# ============================================================================
# DATA QUALITY VALIDATION (NON-INTRUSIVE)
# ============================================================================
from pyspark.sql.functions import udf, struct, col
from pyspark.sql.types import ArrayType, StringType
import re
from datetime import datetime, date

def validate_customers_row(row):
    _dq_errors = []
    _dq_warnings = []

    # ========== REQUIRED FIELD VALIDATION ==========
    required_fields = ["customer_id", "email"]
    for field in required_fields:
        if getattr(row, field) is None or str(getattr(row, field)).strip() == "":
            _dq_errors.append(f"MISSING_REQUIRED_FIELD:{field}")

    # ========== EMAIL FORMAT VALIDATION ==========
    email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    email = getattr(row, "email")
    if email and not re.match(email_pattern, email):
        _dq_errors.append("INVALID_EMAIL_FORMAT:email")

    # ========== PHONE FORMAT VALIDATION (WARNING) ==========
    phone_pattern = r'^[\d\s\-\(\)\+]{7,}$'
    phone = getattr(row, "phone")
    if phone and str(phone).strip() != "" and not re.match(phone_pattern, phone):
        _dq_warnings.append("UNUSUAL_PHONE_FORMAT:phone")

    # ========== DATE VALIDATION ==========
    reg_date = getattr(row, "registration_date")
    _temp_parsed_reg_date = None
    if reg_date and str(reg_date).strip() != "":
        for fmt in ["%Y-%m-%d", "%m/%d/%Y", "%d-%m-%Y"]:
            try:
                _temp_parsed_reg_date = datetime.strptime(reg_date, fmt).date()
                break
            except:
                continue
        if _temp_parsed_reg_date is None:
            _dq_errors.append("INVALID_DATE_FORMAT:registration_date")
        elif _temp_parsed_reg_date > date.today():
            _dq_warnings.append("FUTURE_REGISTRATION_DATE:registration_date")

    # ========== NUMERIC FIELD VALIDATION ==========
    ltv = getattr(row, "lifetime_value")
    if ltv is not None and str(ltv).strip() != "":
        try:
            _temp_ltv_numeric = float(ltv)
            if _temp_ltv_numeric < 0:
                _dq_errors.append("NEGATIVE_VALUE:lifetime_value")
        except:
            _dq_errors.append("INVALID_NUMERIC:lifetime_value")

    # ========== CUSTOMER SEGMENT VALIDATION (WARNING) ==========
    valid_segments = ["basic", "standard", "premium", "vip"]
    segment = getattr(row,"customer_segment")
    if segment and str(segment).strip() != "" and segment.lower() not in valid_segments:
        _dq_warnings.append(f"UNKNOWN_SEGMENT:{segment}")

    # ========== BOOLEAN FIELD VALIDATION ==========
    valid_boolean_values = ["true", "false", "1", "0", "y", "n", "yes", "no"]
    is_active = getattr(row,"is_active")
    if is_active and str(is_active).strip().lower() not in valid_boolean_values:
        _dq_errors.append("INVALID_BOOLEAN:is_active")

    # ========== PII DETECTION (WARNING) ==========
    test_patterns = ["test", "example", "sample", "firstname", "lastname"]
    for field in ["first_name", "last_name"]:
        value = getattr(row,field)
        if value:
            value_lower = value.lower()
            for pattern in test_patterns:
                if pattern in value_lower:
                    _dq_warnings.append(f"POTENTIAL_TEST_DATA:{field}")

    return _dq_errors, _dq_warnings

# Register the UDF
validate_customers_udf = udf(validate_customers_row, ArrayType(ArrayType(StringType())))
    
# ============================================================================
# MAIN PIPELINE CLASS
# ============================================================================


class BronzeCustomerPipeline:

    def __init__(self,metadata_manager=None):
        self.batch_id=str(uuid.uuid4())
        self.ingestion_start=datetime.now()
        self.stats={}
        # self.metadata_manager=metadata_manager

    def run(self,source_file_path:str, write_mode:str):
        print("\n" + "="*70)
        print("BRONZE LAYER INGESTION PIPELINE - CUSTOMERS")
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
                    .csv(source_file_path)
        source_count=df.count()

        self.stats["source_count"]=source_count
        print(f"  ✓ Read {source_count:,} records from source")


        print("[2/7] Detecting Schema Changes...")
        df,schema_changes=handle_schema_changes(df,EXPECTED_SCHEMA_CUSTOMERS_V1)
        self.stats["schema_changes"]=schema_changes


        print("[3/7] Running Data Quality Checks...")
        df = df.withColumn(
            "_dq_result",
            validate_customers_udf(struct([df[x] for x in df.columns]))
        )
        # Split UDF result into separate columns
        df = df.withColumn("_dq_errors", col("_dq_result").getItem(0)) \
                .withColumn("_dq_warnings", col("_dq_result").getItem(1)) \
                .drop("_dq_result")
        # Apply completeness score after UDF
        df = completeness_final_set(df)
        error_count=df.filter(col("_dq_error_count")>0).count()
        total_count=df.count()
        dq_score=((total_count-error_count)/total_count * 100) if total_count>0 else 0

        self.stats["error_count"]=error_count
        self.stats["dq_score"]=dq_score
        try:
            spark.sql(f"""
                CREATE TABLE IF NOT EXISTS {BRONZE_TABLE_CUSTOMERS}
                USING DELTA
                LOCATION '{BRONZE_ROOT_PATH}'
            """)
        except:
            pass

        print(f"  ✓ Validation complete")
        print("[4/7] Detecting Duplicates...")
        df=detect_duplicates_bronze(df,BRONZE_TABLE_CUSTOMERS)
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

    pipeline= BronzeCustomerPipeline()
    df_bronze, stats=pipeline.run(
        source_file_path=source_file_path,
        write_mode="append"
    )



# def validate_customers_bronze_data(df: DataFrame)->DataFrame:
#     df=df.withColumn("_dq_errors",array())
#     df=df.withColumn("_dq_warnings",array())



#     # ========== REQUIRED FIELD VALIDATION ==========
#     required_fields=["customer_id","email"]

#     for field in required_fields:
#         df=df.withColumn(
#             "_dq_errors",
#             when(
#                 col(field).isNull() | (trim(col(field)) ==""),
#                 array_union(
#                     col("_dq_errors"),
#                     array(lit(f"MISSING_REQUIRED_FIELD:{field}"))
#                 )
#             ).otherwise(col("_dq_errors"))

#         )
#     # df.show(truncate=False)


#     # ========== EMAIL FORMAT VALIDATION ==========
#     email_pattern=r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'

#     df=df.withColumn(
#         "_dq_errors",
#         when(
#             col("email").isNotNull() & 
#             ~col("email").rlike(email_pattern),
#             array_union(
#                 col("_dq_errors"),
#                 array(lit("INVALID_EMAIL_FORMAT:email"))
#             )
#         ).otherwise(col("_dq_errors"))
#     )

#     # df.show(truncate=False)


#     # ========== PHONE FORMAT VALIDATION (WARNING) ==========
#     # Various phone formats are acceptable, so just warn if unusual
#     phone_pattern = r'^[\d\s\-\(\)\+]{7,}$'


#     df=df.withColumn(
#         "_dq_warnings",
#         when(
#             col("phone").isNotNull()&
#             (trim(col("phone"))!="") &
#             ~col("phone").rlike(phone_pattern),
#             array_union(
#                 col("_dq_warnings"),
#                 array(lit("UNUSUAL_PHONE_FORMAT:phone"))
#             )
#         ).otherwise(col("_dq_warnings"))
#     )

#     #========= DATE VALIDATION ==========

#     df=df.withColumn(
#         "_temp_parsed_reg_date",
#         coalesce(
#             to_date(col("registration_date"), "yyyy-MM-dd"),
#             to_date(col("registration_date"),"MM/dd/yyyy"),
#             to_date(col("registration_date"),"dd-MM-yyyy")
#         )
#     )

#     df=df.withColumn(
#         "_dq_errors",
#         when(
#             col("_temp_parsed_reg_date").isNull() &
#             col("registration_date").isNotNull() & #this line have bug for the validation
#             (trim(col("registration_date"))!=""),
#             array_union(
#                 col("_dq_errors"),
#                 array(lit("INVALID_DATE_FORMAT:registration_date"))
#             )
#         ).otherwise(col("_dq_errors"))
#     )

#     # Check if registration date is in the future

#     df=df.withColumn(
#         "_dq_warnings",
#         when(
#             col("_temp_parsed_reg_date")>current_date(),
#             array_union(
#                 col("_dq_warnings"),
#                 array(lit("FUTURE_REGISTRATION_DATE:registration_date"))
#             )
#         ).otherwise(col("_dq_warnings"))
#     ).drop("_temp_parsed_reg_date")

#     # ========== NUMERIC FIELD VALIDATION ==========
#     df = df.withColumn(
#         "_temp_ltv_numeric",
#         col("lifetime_value").cast("decimal(12,2)")
#     )
    
#     df = df.withColumn(
#         "_dq_errors",
#         when(
#             col("_temp_ltv_numeric").isNull() & 
#             col("lifetime_value").isNotNull() &
#             (trim(col("lifetime_value")) != ""),
#             array_union(
#                 col("_dq_errors"),
#                 array(lit("INVALID_NUMERIC:lifetime_value"))
#             )
#         ).otherwise(col("_dq_errors"))
#     )
#     # Check for negative lifetime value
#     df = df.withColumn(
#         "_dq_errors",
#         when(
#             col("_temp_ltv_numeric") < 0,
#             array_union(
#                 col("_dq_errors"),
#                 array(lit("NEGATIVE_VALUE:lifetime_value"))
#             )
#         ).otherwise(col("_dq_errors"))
#     ).drop("_temp_ltv_numeric")

#     # ========== CUSTOMER SEGMENT VALIDATION (WARNING) ==========
#     valid_segments = ["basic", "standard", "premium", "vip"]


#     df=df.withColumn(
#         "_dq_warnings",
#         when(
#             col("customer_segment").isNotNull() &
#             (trim(col("customer_segment"))!="") &
#             ~lower(col("customer_segment")).isin(valid_segments),
#             array_union(
#                 col("_dq_warnings"),
#                 array(lit(f"UNKNOWN_SEGMENT:{col('customer_segment')}"))
#             )
#         ).otherwise(col("_dq_warnings"))
#     )
#     # ========== BOOLEAN FIELD VALIDATION ==========
#     # is_active can be: True, False, true, false, 1, 0, Y, N, Yes, No
#     valid_boolean_values = ["true", "false", "1", "0", "y", "n", "yes", "no"]
    
#     df = df.withColumn(
#         "_dq_errors",
#         when(
#             col("is_active").isNotNull() &
#             (trim(col("is_active")) != "") &
#             ~lower(trim(col("is_active"))).isin(valid_boolean_values),
#             array_union(
#                 col("_dq_errors"),
#                 array(lit("INVALID_BOOLEAN:is_active"))
#             )
#         ).otherwise(col("_dq_errors"))
#     )

#     # ========== PII DETECTION (WARNING) ==========
#     # Warn if first_name or last_name looks like test data
#     test_patterns = ["test", "example", "sample", "firstname", "lastname"]
    
#     for field in ["first_name", "last_name"]:
#         for pattern in test_patterns:
#             df = df.withColumn(
#                 "_dq_warnings",
#                 when(
#                     lower(col(field)).contains(pattern),
#                     array_union(
#                         col("_dq_warnings"),
#                         array(lit(f"POTENTIAL_TEST_DATA:{field}"))
#                     )
#                 ).otherwise(col("_dq_warnings"))
#             )

#     # ========== CALCULATE COMPLETENESS SCORE ==========
#     df=completeness_final_set(df)
#     return df











