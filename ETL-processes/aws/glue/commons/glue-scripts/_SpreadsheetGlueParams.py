import sys
import boto3
from urllib.parse import urlparse
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import os

# import main modules from custom S3 Python library path)
from SpreadsheetIngester import execute_pipeline    # main pipeline process
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

required_params = ['connection_name', 'target_table', 'folder_location', 'input_file_name', 'audit_table', 'passed_parameter']
missing_params = [p for p in required_params if f"--{p}" not in sys.argv]

if missing_params:
    missing_str = ", ".join(missing_params)
    print(f"### -CRITICAL: Missing required parameters: {missing_str}")
    raise_custom_error("missing_parameters", f"Job failed to start. Missing parameters: {missing_str}")

expected_args = [
    'JOB_NAME', 
    'passed_parameter', 
    'input_file_name',
    'folder_location',
    'connection_name',
    'target_table',
    'audit_table'
]

args = getResolvedOptions(sys.argv, expected_args)
args['target_headers'] = get_optional_arg('target_headers')
args['row_number_label'] = get_optional_arg('row_number_label')


# starts here
s3_start_URI = "s3://"
input_file_folder = "inbound/"
success_file_folder = "processed/"
error_file_folder = "error/"

file_name_passed = os.path.splitext(args.get('input_file_name'))[0]
file_ext_passed = os.path.splitext(args.get('input_file_name'))[1]
file_name_param = file_name_passed[:-2] + args.get('passed_parameter') + file_ext_passed

file_path = s3_start_URI + args.get('folder_location') + input_file_folder

args['input_file_path'] = file_path

args['input_file_name'] = file_name_param
args['target_table'] = args.get('target_table')[:-2] + args.get('passed_parameter')


sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

try:
    execute_pipeline(args, spark, glueContext) # main glue processes
    success_path = s3_start_URI + args.get('folder_location') + success_file_folder + file_name_param
    move_s3_file(file_path + file_name_param, success_path) # success path move
    
except Exception as main_error:
    print(f"### -Job Interrupted/Failed: {str(main_error)}")
    
    # attempt to quarantine the file
    try:
        error_path = s3_start_URI + args.get('folder_location') + error_file_folder + file_name_param
        move_s3_file(file_path + file_name_param, error_path)
        
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