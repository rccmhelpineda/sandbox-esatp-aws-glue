import os
import time

def generate_uuidv7():
    """
    **Generates a strictly compliant UUIDv7 
    (Timestamp first, random second)
    """
    t_ms = int(time.time() * 1000)
    t_hex = f"{t_ms:012x}"

    # 12-bit random a + version 7
    r_a = os.urandom(2).hex()
    r_a = f"7{r_a[1:]}"

    # 62-bit random b + variant 10
    r_b = os.urandom(8).hex()
    r_b = f"{hex(int(r_b[0], 16) & 0x3 | 0x8)[2:]}{r_b[1:]}"
    
    return f"{t_hex[:8]}-{t_hex[8:]}-{r_a}-{r_b[:4]}-{r_b[4:]}"


def backup_df_to_s3_csv(df, s3_path):
    """
    **Coalesces and writes a PySpark DataFrame to S3 as a CSV.
    """
    print(f"### - Attempting to write CSV backup to S3 at: {s3_path}")

    try:
        df.coalesce(1).write \
            .mode("overwrite") \
            .option("header", "true") \
            .csv(s3_path)
        print("### - SUCCESS: S3 CSV backup completed.")
        
    except Exception as e:
        error_msg = f"Failed to write CSV backup to S3 path {s3_path}. Reason: {str(e)}"
        print(f"### - CRITICAL S3 ERROR: {error_msg}")

        """
        **Rraise exception here so the main runner script catches it then,
        triggers the Step Functions error payload.
        """
        raise Exception(error_msg)