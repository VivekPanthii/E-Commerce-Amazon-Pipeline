from pyspark.sql import SparkSession


def get_spark_session (appname: str="ECommercePipeline", master: str="local[*]")-> SparkSession:
    spark=SparkSession\
            .builder\
            .appName(appname)\
            .master(master)\
            .config("spark.sql.codegen.wholeStage", "true" )\
            .config("spark.jars.packages","io.delta:delta-spark_2.13:4.0.0")\
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")\
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")\
            .getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")

    return spark


def stop_spark(spark):
    if spark:
        spark.stop()
