import sys
import boto3
import time
import json
import concurrent.futures

# import your custom error library
from CustomErrorLibs import raise_custom_error

# initialize the Glue client
glue_client = boto3.client('glue')

def run_and_monitor_job(job_name):

    """
    **Triggers a Glue job by name & 
    polls until it reaches a terminal state.
    """

    try:
        response = glue_client.start_job_run(JobName=job_name)
        job_run_id = response['JobRunId']
        
        while True:
            status_response = glue_client.get_job_run(JobName=job_name, RunId=job_run_id)
            state = status_response['JobRun']['JobRunState']
            
            if state in ['SUCCEEDED', 'FAILED', 'STOPPED', 'TIMEOUT']:
                error_message = None
                if state != 'SUCCEEDED':
                    error_message = status_response['JobRun'].get('ErrorMessage', 'No error message provided.')
                
                return {
                    "JobName": job_name,
                    "JobRunId": job_run_id,
                    "Status": state,
                    "Error": error_message
                }
            
            time.sleep(30)
            
    except Exception as e:
         return {
             "JobName": job_name,
             "Status": "LAUNCH_FAILED",
             "Error": str(e)
         }

def main():
    jobs_to_run = [
        "MyBSS_Bayan_EOC11_P10_SAP-glbilled-27",
        "MyBSS_Bayan_EOC10_P10_SAP-glbilled-24",
        "MyBSS_Bayan_EOC09_P10_SAP-glbilled-21",
        "MyBSS_Bayan_EOC08_P10_SAP-glbilled-18",
        "MyBSS_Bayan_EOC07_P10_SAP-glbilled-16",
        "MyBSS_Bayan_EOC06_P10_SAP-glbilled-13",
        "MyBSS_Bayan_EOC05_P10_SAP-glbilled-11",
        "MyBSS_Bayan_EOC04_P10_SAP-glbilled-10",
        "MyBSS_Bayan_EOC03_P10_SAP-glbilled-08",
        "MyBSS_Bayan_EOC02_P10_SAP-glbilled-06",
        "MyBSS_Bayan_EOC01_P10_SAP-glbilled-01"
    ]
    
    results = []
    
    print(f"### -Starting {len(jobs_to_run)} Glue jobs concurrently...")
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(jobs_to_run)) as executor:
        future_to_job = {
            executor.submit(run_and_monitor_job, job_name): job_name 
            for job_name in jobs_to_run
        }
        
        for future in concurrent.futures.as_completed(future_to_job):
            results.append(future.result())
            
    # print the final consolidated report to CloudWatch
    print("\n--- CONSOLIDATED ETL RUN REPORT ---")
    report_json = json.dumps(results, indent=4)
    print(report_json)
    
    # filter out jobs that did not succeed
    failed_jobs = [r for r in results if r["Status"] != "SUCCEEDED"]
    
    if failed_jobs:
        print(f"\n### -CRITICAL: {len(failed_jobs)} out of {len(jobs_to_run)} child Glue jobs failed.")
        
        # consolidate the error details for each failed job
        failure_summary = "; ".join([f"[{job['JobName']}]: {job['Error']}" for job in failed_jobs])
        
        # raise custom error wrapped in GLUE_ORCH_CUSTOM_PAYLOAD for Step Functions
        raise_custom_error(
            status="orchestration_child_failure",
            reason=f"One or more child ETL jobs failed. Failures -> {failure_summary}",
            input_path="Multiple",
            custom_wrapper="GLUE_ORCH_CUSTOM_PAYLOAD"
        )
    else:
        print("\n### -All child Glue jobs completed successfully.")

if __name__ == "__main__":
    main()