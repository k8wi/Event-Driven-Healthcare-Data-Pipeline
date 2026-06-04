from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, row_number, lit, concat, hash
from pyspark.sql.types import IntegerType, BooleanType
import logging
import sys

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    spark = SparkSession.builder \
        .appName("stage-3-scd-type2") \
        .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
        .config("spark.hadoop.google.cloud.auth.type", "APPLICATION_DEFAULT_CREDENTIALS") \
        .getOrCreate()
    
    logger.info("ðŸ”„ Stage 3: SCD Type 2 Dimensions...")
    
    try:
        prescribers = spark.read.parquet("gs://cms-partd-data-demo/processed/staging/prescribers/")
        drug_claims = spark.read.parquet("gs://cms-partd-data-demo/processed/staging/drug_claims/")
        
        # Create dim_provider
        dim_provider = prescribers.select(
            hash(col("npi")).cast(IntegerType()).alias("provider_key"),
            col("npi"),
            col("first_name"),
            col("last_name"),
            col("city"),
            col("state"),
            col("zip"),
            lit(True).cast(BooleanType()).alias("is_active")
        )
        
        dim_provider.write.mode("overwrite").parquet("gs://cms-partd-data-demo/processed/facts/dim_provider/")
        logger.info(f"âœ… Created dim_provider: {dim_provider.count():,} records")
        
        # Create dim_drug
        dim_drug = drug_claims.select("brand_name").dropDuplicates().select(
            hash(col("brand_name")).cast(IntegerType()).alias("drug_key"),
            col("brand_name"),
            lit(True).cast(BooleanType()).alias("is_active")
        )
        
        dim_drug.write.mode("overwrite").parquet("gs://cms-partd-data-demo/processed/facts/dim_drug/")
        logger.info(f"âœ… Created dim_drug: {dim_drug.count():,} records")
        
        # Create fact_claims
        fct_claims = drug_claims.join(
            dim_provider.select("provider_key", "npi"),
            on="npi",
            how="left"
        ).join(
            dim_drug.select("drug_key", "brand_name"),
            on="brand_name",
            how="left"
        ).select(
            hash(concat(col("npi"), col("brand_name"))).cast(IntegerType()).alias("claim_id"),
            col("provider_key"),
            col("drug_key"),
            col("beneficiary_count"),
            col("total_drug_cost")
        )
        
        fct_claims.write.mode("overwrite").parquet("gs://cms-partd-data-demo/processed/facts/fct_claims/")
        logger.info(f"âœ… Created fct_claims: {fct_claims.count():,} records")
        
    except Exception as e:
        logger.error(f"âŒ Stage 3 failed: {e}", exc_info=True)
        sys.exit(1)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()