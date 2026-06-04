from pyspark.sql import SparkSession
import logging
import sys

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    spark = SparkSession.builder \
        .appName("stage-1-raw-ingestion") \
        .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
        .config("spark.hadoop.google.cloud.auth.type", "APPLICATION_DEFAULT_CREDENTIALS") \
        .getOrCreate()
    
    logger.info("ðŸš€ Stage 1: Loading CSV from GCS...")
    
    try:
        # Read CSV
        df = spark.read \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .csv("gs://cms-partd-data-demo/2023/*.csv")
        
        row_count = df.count()
        logger.info(f"âœ… Loaded {row_count:,} rows")
        
        # Lowercase columns
        for col_name in df.columns:
            df = df.withColumnRenamed(col_name, col_name.lower())
        
        # Write Parquet
        df.write.mode("overwrite").parquet("gs://cms-partd-data-demo/processed/raw/")
        logger.info(f"âœ… Written to gs://cms-partd-data-demo/processed/raw/")
        
    except Exception as e:
        logger.error(f"âŒ Stage 1 failed: {e}", exc_info=True)
        sys.exit(1)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()