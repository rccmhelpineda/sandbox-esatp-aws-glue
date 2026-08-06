import sys
import boto3
from urllib.parse import urlparse
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

# import main modules from custom S3 Python library path
import SpreadsheetIngester
import TextIngester
from CustomErrorLibs import raise_custom_error      # further helpful for Step Functions integration
from UtilsAWS import move_s3_file                   # if the pipeline fails, attempt to move the file to error path.

# ===============================================================================
# --- PARAMETER PARSING & VALIDATION ---
# ===============================================================================
def get_optional_arg(arg_name):
    for i in range(len(sys.argv)):
        if sys.argv[i].startswith("--") and len(sys.argv) > i + 1:
            if sys.argv[i][2:] == arg_name:
                return sys.argv[i + 1]
    return ""

required_params = ['input_file_path', 'target_table', 'error_file_path']
missing_params = [p for p in required_params if f"--{p}" not in sys.argv]

if missing_params:
    missing_str = ", ".join(missing_params)
    print(f"### -CRITICAL: Missing required parameters: {missing_str}")
    raise_custom_error("missing_parameters", f"Job failed to start. Missing parameters: {missing_str}")

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'input_file_path', 'connection_name', 'target_table', 'error_file_path', 'processed_file_path', 'input_file_name'])

# Extract optional parameters
args['target_headers'] = get_optional_arg('target_headers')
args['row_number_label'] = get_optional_arg('row_number_label')
args['ingestion_type'] = get_optional_arg('ingestion_type')


# ===============================================================================
# --- INITIALIZE SPARK/GLUE ---
# ===============================================================================
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

try:
    # ===========================================================================
    # --- PIPELINE ROUTER ---
    # ===========================================================================
    # Check the ingestion_type parameter to decide which script to run
    ingestion_type = args.get('ingestion_type', '').upper()
    
    if ingestion_type in ['TXT', 'TXT_NUMBERED']:
        print("### -Routing to Text Ingestion Pipeline...")
        TextIngester.execute_pipeline(args, spark, glueContext)
    else:
        print("### -Routing to Spreadsheet Ingestion Pipeline...")
        SpreadsheetIngester.execute_pipeline(args, spark, glueContext) 
        
    # success path move
    move_s3_file(args.get('input_file_path')+args.get('input_file_name'), args.get('processed_file_path')+args.get('input_file_name'))
    
except Exception as main_error:
    print(f"### -Job Interrupted/Failed: {str(main_error)}")
    
    # attempt to quarantine the file
    try:
        move_s3_file(args.get('input_file_path')+args.get('input_file_name'), args.get('error_file_path')+args.get('input_file_name'))
        
    except Exception as s3_error:
        print("### -CRITICAL: Cascading failure. File quarantine failed after pipeline failure.")
        raise_custom_error(
            "cascading_failure", 
            f"Pipeline Error: {str(main_error)} | S3 Quarantine Error: {str(s3_error)}"
        )
    
    # if the file moved successfully, raise the original pipeline process error
    raise main_error
    
finally:
    job.commit()