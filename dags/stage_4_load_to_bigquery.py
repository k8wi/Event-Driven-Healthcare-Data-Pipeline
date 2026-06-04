from pyspark.sql import SparkSession
import logging
import sys

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main():
    spark = (
        SparkSession.builder
        .appName("stage-4-load-to-bigquery")
        .config(
            "spark.hadoop.fs.gs.impl",
            "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem"
        )
        .config(
            "spark.hadoop.google.cloud.auth.type",
            "APPLICATION_DEFAULT_CREDENTIALS"
        )
        .getOrCreate()
    )

    logger.info("ðŸ“¤ Stage 4: Loading Parquet to BigQuery...")

    PROJECT_ID = "gen-lang-client-0215307817"
    TEMP_BUCKET = "cms-partd-data-demo"

    try:
        # Load stg_prescribers
        stg_prescribers = spark.read.parquet(
            "gs://cms-partd-data-demo/processed/staging/prescribers/"
        )

        stg_prescribers.write \
            .format("bigquery") \
            .mode("overwrite") \
            .option("temporaryGcsBucket", TEMP_BUCKET) \
            .option(
                "table",
                f"{PROJECT_ID}:staging_layer.stg_prescribers"
            ) \
            .save()

        logger.info(
            f"âœ… Loaded {stg_prescribers.count():,} rows to stg_prescribers"
        )

        # Load stg_drug_claims
        stg_drug_claims = spark.read.parquet(
            "gs://cms-partd-data-demo/processed/staging/drug_claims/"
        )

        stg_drug_claims.write \
            .format("bigquery") \
            .mode("overwrite") \
            .option("temporaryGcsBucket", TEMP_BUCKET) \
            .option(
                "table",
                f"{PROJECT_ID}:staging_layer.stg_drug_claims"
            ) \
            .save()

        logger.info(
            f"âœ… Loaded {stg_drug_claims.count():,} rows to stg_drug_claims"
        )

        # Load dim_provider
        dim_provider = spark.read.parquet(
            "gs://cms-partd-data-demo/processed/facts/dim_provider/"
        )

        dim_provider.write \
            .format("bigquery") \
            .mode("overwrite") \
            .option("temporaryGcsBucket", TEMP_BUCKET) \
            .option(
                "table",
                f"{PROJECT_ID}:dim_tables.dim_provider"
            ) \
            .save()

        logger.info(
            f"âœ… Loaded {dim_provider.count():,} rows to dim_provider"
        )

        # Load dim_drug
        dim_drug = spark.read.parquet(
            "gs://cms-partd-data-demo/processed/facts/dim_drug/"
        )

        dim_drug.write \
            .format("bigquery") \
            .mode("overwrite") \
            .option("temporaryGcsBucket", TEMP_BUCKET) \
            .option(
                "table",
                f"{PROJECT_ID}:dim_tables.dim_drug"
            ) \
            .save()

        logger.info(
            f"âœ… Loaded {dim_drug.count():,} rows to dim_drug"
        )

        # Load fct_claims
        fct_claims = spark.read.parquet(
            "gs://cms-partd-data-demo/processed/facts/fct_claims/"
        )

        fct_claims.write \
            .format("bigquery") \
            .mode("overwrite") \
            .option("temporaryGcsBucket", TEMP_BUCKET) \
            .option(
                "table",
                f"{PROJECT_ID}:fact_tables.fct_claims"
            ) \
            .save()

        logger.info(
            f"âœ… Loaded {fct_claims.count():,} rows to fct_claims"
        )

        logger.info("âœ… ALL DATA LOADED TO BIGQUERY!")

    except Exception as e:
        logger.error(f"âŒ Stage 4 failed: {e}", exc_info=True)
        sys.exit(1)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()