from pyspark.sql import DataFrame
from datetime import datetime
from typing import Dict, Tuple
from pyspark.sql.functions import *
from pyspark.sql.types import *

def schema_change_detect(df:DataFrame, expected_schema: Dict)->Dict:
    source_columns=set(df.columns)
    expected_columns=set(f["name"] for f in expected_schema["fields"])


    new_columns=source_columns-expected_columns
    missing_columns=expected_columns-source_columns
    changes={
        "has_changes": len(new_columns)>0 or len(missing_columns)>0,
        "new_columns": list(new_columns),
        "missing_columns": list(missing_columns),
        "detected_at":datetime.now().isoformat(),
        "source_column_count":len(source_columns),
        "expected_column_count":len(expected_columns)
    }
    return(changes)

def handle_schema_changes(df:DataFrame, expected_schema:Dict)-> Tuple[DataType,Dict]:

    schema_changes=schema_change_detect(df,expected_schema)

    if schema_changes["has_changes"]:
        print(f"      Schema changes detected!")
        print(f"      New columns: {schema_changes['new_columns']}")
        print(f"      Missing columns: {schema_changes['missing_columns']}")

        if schema_changes["has_changes"]:
            extra_columns=schema_changes["new_columns"]
            df=df.withColumn(
                "_extra_fields",
                to_json(struct(*[col(c) for c in extra_columns]))
            )
            df=df.drop(*extra_columns)
        else:
            df=df.withColumn("_extra_fields",lit(None).cast("string"))
        
        if schema_changes["missing_columns"]:
            for col_name in schema_changes["missing_columns"]:
                df=df.withColumn(col_name,lit(None).cast("string"))
    else:
        df=df.withColumn("_extra_fields", lit(None).cast("string"))
        print(f"  ✓ Schema matches expected")

    return df, schema_changes