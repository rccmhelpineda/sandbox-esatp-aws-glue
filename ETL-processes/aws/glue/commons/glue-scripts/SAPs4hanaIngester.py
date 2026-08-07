import sys
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import col, lit, trim, regexp_replace, length, max as spark_max

# Import your new utility function
from Utils import backup_df_to_s3_csv

def execute_pipeline(args, spark, glueContext):
    """
    Main ETL Pipeline for SAP S4HANA Database Migration.
    Aligns schemas dynamically, sanitizes strings, and truncates/inserts data.
    """
    target_dbtable = args['SCHEMA_TBL_TO']
    
    # ===============================================================================
    # --- STEP 1: EXTRACT FROM SOURCE DB ---
    # ===============================================================================
    print(f"### - Extracting data from source table {args['SCHEMA_DB_FROM']}...")

    source_dyf = glueContext.create_dynamic_frame.from_options(
        connection_type="postgresql",
        connection_options={
            "useConnectionProperties": "true",
            "connectionName": args['CONN_DB_FROM'],
            "dbtable": args['SCHEMA_DB_FROM']
        },
        transformation_ctx="source_dyf_extract"
    )

    source_df = source_dyf.toDF()

    if source_df.count() == 0:
        print("### - WARNING: Source table is empty. Pipeline ending gracefully.")
        return

    # ===============================================================================
    # --- STEP 2: ORDINAL POSITION & CASING ALIGNMENT ---
    # ===============================================================================
    print("### - Fetching destination table schema for ordinal alignment...")

    dest_schema_dyf = glueContext.create_dynamic_frame.from_options(
        connection_type="postgresql",
        connection_options={
            "useConnectionProperties": "true",
            "connectionName": args['CONN_DB_TO'],
            "dbtable": target_dbtable
        },
        transformation_ctx="dest_schema_dyf"
    )

    dest_ordered_cols = dest_schema_dyf.toDF().columns
    source_col_map = {c.lower(): c for c in source_df.columns}
    aligned_exprs = []

    for dest_col in dest_ordered_cols:
        dest_lower = dest_col.lower()
        if dest_lower in source_col_map:
            actual_source_col = source_col_map[dest_lower]
            aligned_exprs.append(col(actual_source_col).alias(dest_col))
        else:
            print(f"### - NOTICE: Target column '{dest_col}' missing in source. Filling with NULL.")
            aligned_exprs.append(lit(None).cast("string").alias(dest_col))

    aligned_df = source_df.select(*aligned_exprs)

    # --- DATA SANITIZATION ---
    for c in aligned_df.columns:
        aligned_df = aligned_df.withColumn(c, trim(col(c).cast("string")))

    if "vatrate" in aligned_df.columns:
        aligned_df = aligned_df.withColumn("vatrate", regexp_replace(col("vatrate"), "\\.0*$", ""))

    # --- PRE-FLIGHT LENGTH AUDIT ---
    print("### - AUDIT: Max character lengths per column before DB write:")
    for c in aligned_df.columns:
        max_len = aligned_df.select(spark_max(length(col(c)))).collect()[0][0]
        print(f"  -> Column '{c}': max length = {max_len}")

    # ===============================================================================
    # --- STEP 3: WRITE DATAFRAME TO S3 AS CSV (UTILITY CALL) ---
    # ===============================================================================
    if not args.get('FOLDER_LOCATION'):
        print("### - WARNING: No FOLDER_LOCATION provided. Skipping S3 backup.")
        return
    
    s3_start_URI = "s3://"
    input_file_folder = "inbound/"
    s3_path = s3_start_URI + args.get('FOLDER_LOCATION') + input_file_folder

    backup_df_to_s3_csv(aligned_df, s3_path)

    # ===============================================================================
    # --- STEP 4: LOAD INTO DESTINATION AURORA DB (TRUNCATE & INSERT) ---
    # ===============================================================================
    print(f"### - Writing data to destination table {target_dbtable} via native PySpark...")

    jdbc_conf = glueContext.extract_jdbc_conf(args['CONN_DB_TO'])
    base_url = jdbc_conf["url"].rstrip("/")
    jdbc_url = f"{base_url}/postgres" if not base_url.endswith("postgres") else base_url

    connection_properties = {
        "user": jdbc_conf.get("user", ""),
        "password": jdbc_conf.get("password", ""),
        "driver": "org.postgresql.Driver",
        "stringtype": "unspecified"
    }

    aligned_df.write \
        .option("truncate", "true") \
        .mode("overwrite") \
        .jdbc(
            url=jdbc_url,
            table=target_dbtable,
            properties=connection_properties
        )

    print("### - SUCCESS: Destination table truncated and freshly loaded.")