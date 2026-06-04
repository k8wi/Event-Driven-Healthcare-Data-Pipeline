from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, upper, coalesce, lit
from pyspark.sql.types import IntegerType, FloatType
import logging
import sys

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    spark = SparkSession.builder \
        .appName("stage-2-data-cleansing") \
        .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
        .config("spark.hadoop.google.cloud.auth.type", "APPLICATION_DEFAULT_CREDENTIALS") \
        .getOrCreate()
    
    logger.info("ðŸ§¹ Stage 2: Data Cleansing...")
    
    try:
        raw_df = spark.read.parquet("gs://cms-partd-data-demo/processed/raw/")
        logger.info(f"ðŸ“– Read {raw_df.count():,} raw records")
        
        # Print actual columns for debugging
        logger.info(f"Columns: {raw_df.columns}")
        
        # Transform prescribers (use actual column names)
        prescribers = raw_df.select(
            col("prscrbr_npi").cast(IntegerType()).alias("npi"),
            upper(trim(coalesce(col("prscrbr_first_name"), lit("")))).alias("first_name"),
            upper(trim(coalesce(col("prscrbr_last_org_name"), lit("")))).alias("last_name"),
            upper(trim(coalesce(col("prscrbr_city"), lit("")))).alias("city"),
            upper(trim(coalesce(col("prscrbr_state_abrvtn"), lit("")))).alias("state"),
            lit("").alias("zip")
        ).dropDuplicates(["npi"])
        
        prescribers.write.mode("overwrite").parquet("gs://cms-partd-data-demo/processed/staging/prescribers/")
        logger.info(f"âœ… Written {prescribers.count():,} prescriber records")
        
        # Transform drug claims
        drug_claims = raw_df.select(
            col("prscrbr_npi").cast(IntegerType()).alias("npi"),
            upper(trim(col("brnd_name"))).alias("brand_name"),
            col("tot_benes").cast(IntegerType()).alias("beneficiary_count"),
            col("tot_drug_cst").cast(FloatType()).alias("total_drug_cost")
        ).filter(col("total_drug_cost") > 0)
        
        drug_claims.write.mode("overwrite").parquet("gs://cms-partd-data-demo/processed/staging/drug_claims/")
        logger.info(f"âœ… Written {drug_claims.count():,} drug claim records")
        
    except Exception as e:
        logger.error(f"âŒ Stage 2 failed: {e}", exc_info=True)
        sys.exit(1)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()