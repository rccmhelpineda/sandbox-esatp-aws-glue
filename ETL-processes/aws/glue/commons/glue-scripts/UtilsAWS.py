import boto3
from urllib.parse import urlparse

def move_s3_file(src_url, dest_url):

    """ 
    **Moves an S3 file using boto3. 
    Skips if destination is blank or file is missing.
    """

    if not src_url or not dest_url:
        return
        
    print(f"### -Attempting to move file from {src_url} to {dest_url}")
    try:
        s3 = boto3.client('s3')
        src_parsed = urlparse(src_url)
        dest_parsed = urlparse(dest_url)
        
        src_bucket, src_key = src_parsed.netloc, src_parsed.path.lstrip('/')
        dest_bucket, dest_key = dest_parsed.netloc, dest_parsed.path.lstrip('/')
        
        # check if the file actually exists before trying to move it
        try:
            s3.head_object(Bucket=src_bucket, Key=src_key)

        except Exception:
            error_msg = "Source file does not exist in S3. Skipping move operation."
            print(f"### -{error_msg}")
            raise Exception(error_msg) # raise it so the main script catches it as a cascading failure
            
        # perform Copy then Delete (S3 Native Move)
        s3.copy_object(Bucket=dest_bucket, Key=dest_key, CopySource={'Bucket': src_bucket, 'Key': src_key})
        s3.delete_object(Bucket=src_bucket, Key=src_key)
        print("### -File successfully moved to processed path.")
        
    except Exception as e:
        error_msg = f"Failed to move file to error path. Reason: {str(e)}"
        print(f"### -Warning: {error_msg}")
        raise Exception(error_msg) # raise it so the main script catches it as a cascading failure