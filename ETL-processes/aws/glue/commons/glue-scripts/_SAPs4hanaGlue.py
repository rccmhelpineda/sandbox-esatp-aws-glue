import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

# Import main modules from custom S3 Python library path
from SAPs4hanaIngester import execute_pipeline
from CustomErrorLibs import raise_custom_error

# ===============================================================================
# --- PARAMETER PARSING & VALIDATION ---
# ===============================================================================
expected_args = [
    'JOB_NAME',
    'CONN_DB_FROM',
    'CONN_DB_TO',
    'FOLDER_LOCATION',
    'SCHEMA_DB_FROM',
    'SCHEMA_TBL_TO'
]

# Check for missing parameters before starting
missing_params = [p for p in expected_args if f"--{p}" not in sys.argv]
if missing_params:
    missing_str = ", ".join(missing_params)
    print(f"### -CRITICAL: Missing required parameters: {missing_str}")
    # Fail fast if parameters are missing
    raise_custom_error("missing_parameters", f"Job failed to start. Missing: {missing_str}", "Database Source")

args = getResolvedOptions(sys.argv, expected_args)

# ===============================================================================
# --- SPARK & GLUE INITIALIZATION ---
# ===============================================================================
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# ===============================================================================
# --- MAIN PIPELINE EXECUTION ---
# ===============================================================================
try:
    # Call the reusable ETL logic from your custom S3 library
    execute_pipeline(args, spark, glueContext)
    
except Exception as main_error:
    print(f"### -Job Interrupted/Failed: {str(main_error)}")
    
    # Package the error securely for Step Functions to catch
    raise_custom_error(
        status="pipeline_failure", 
        reason=f"Database Migration Error: {str(main_error)}",
        input_path=args['SCHEMA_DB_FROM']
    )
    
finally:
    job.commit()