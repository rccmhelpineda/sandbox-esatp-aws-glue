import sys
import os
import re
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
from pyspark.sql.utils import AnalysisException
from pyspark.sql.window import Window

# Import your modular custom libraries
from CustomErrorLibs import raise_custom_error
from CustomDbConnLibs import get_postgres_jdbc_conf

# ===============================================================================
# --- HELPER FUNCTIONS & FUZZY MATCHING LOGIC ---
# ===============================================================================
def norm(val):
    if val is None:
        return ""
    val_str = str(val).strip()
    if val_str.lower() == "none" or not val_str:
        return ""
    return re.sub(r'\s+', ' ', val_str).strip()

def optimized_header_match(sample_rows, target_headers, num_cols):
    best_mapping = {}
    best_count = 0
    header_end_index = -1
    
    for start_r in range(min(15, len(sample_rows))):
        for window_size in range(1, 10):
            end_r = start_r + window_size
            if end_r > len(sample_rows):
                break
            
            col_strs = {}
            for c in range(num_cols):
                col_parts = [norm(sample_rows[r][c]) for r in range(start_r, end_r)]
                combined = norm(" ".join([p for p in col_parts if p]))
                if combined:
                    col_strs[c] = combined
            
            scores = []
            for c, c_str in col_strs.items():
                for th in target_headers:
                    c_clean = c_str.lower().replace(" ", "").replace("-", "")
                    th_clean = th.lower().replace(" ", "").replace("-", "")
                    
                    if c_clean == th_clean:
                        score = 1.0
                    elif th_clean in c_clean or c_clean in th_clean:
                        score = 0.8
                    elif len(set(c_clean) & set(th_clean)) >= 3:
                        score = 0.7
                    else:
                        score = 0.0
                    
                    if score >= 0.7:
                        scores.append((score, c, th))
            
            scores.sort(key=lambda x: x[0], reverse=True)
            current_mapping = {}
            used_cols = set()
            used_targets = set()
            
            for score, c, th in scores:
                if c not in used_cols and th not in used_targets:
                    current_mapping[c] = th
                    used_cols.add(c)
                    used_targets.add(th)
            
            if len(current_mapping) > best_count:
                best_count = len(current_mapping)
                header_end_index = end_r - 1
                best_mapping = current_mapping
    
    return best_mapping, header_end_index, best_count


# ===============================================================================
# --- MAIN PIPELINE EXECUTION ---
# ===============================================================================
def execute_pipeline(args, spark, glueContext):
    
    def get_text_arg(name, default_val):
        for i, val in enumerate(sys.argv):
            if val == f"--{name}" and len(sys.argv) > i + 1:
                return sys.argv[i + 1]
        return default_val

    # Replaced column labels with target headers
    target_headers_raw = get_text_arg('target_headers', '')
    delimiter_regex = get_text_arg('delimiter_regex', ',') # Default to comma for CSVs
    
    row_num_label = args.get('row_number_label', '').lower()
    input_file_name = args['input_file_name']
    input_path = args['input_file_path'] + input_file_name
    connection_name = args['connection_name']
    target_raw = args['target_table']

    if not target_headers_raw:
        raise_custom_error("missing_parameters", "Job requires --target_headers parameter", input_path)
    
    target_headers_list = [norm(h) for h in target_headers_raw.split(",") if norm(h)]

    # ===============================================================================
    # --- READ CSV DATA ---
    # ===============================================================================
    print(f"### -Reading file: {input_path}")
    try:
        # Read as CSV natively using the provided delimiter
        spark_df = spark.read \
            .option("header", "false") \
            .option("delimiter", delimiter_regex) \
            .csv(input_path)
    except AnalysisException as e:
        if "Path does not exist" in str(e):
            raise_custom_error("missing_file", "S3 object not found", input_path)
        else:
            raise e

    # Stamp original order immediately
    window_spec = Window.orderBy(F.lit(1))
    spark_df = spark_df.withColumn("_temp_row_num", F.row_number().over(window_spec))

    # ===============================================================================
    # --- DYNAMIC HEADER MATCHING & FILTERING ---
    # ===============================================================================
    data_cols = [c for c in spark_df.columns if c != "_temp_row_num"]
    num_cols = len(data_cols)
    
    # Take a sample to find headers
    sample_df = spark_df.limit(50)
    all_rows = sample_df.collect()

    col_mapping, header_end_index, best_match_count = optimized_header_match(all_rows, target_headers_list, num_cols)
    print(f"### -Universal Fuzzy Detector matched {best_match_count}/{len(target_headers_list)} headers ending at row index {header_end_index}.")

    if header_end_index >= 0 and col_mapping:
        # Data begins immediately after the header row
        data_start_index = header_end_index + 1
        
        # Filter out the headers and preamble metadata
        data_df = spark_df.filter(F.col("_temp_row_num") > data_start_index)
        
        # Identify footer keywords and empty rows
        footer_keywords = ['report creator', 'page ', 'run time', 'generated by', 'end of report', 'confidential', 'rows selected']
        footer_pattern = "(?i)(" + "|".join(footer_keywords) + ")"
        
        footer_condition = F.lit(False)
        non_empty_condition = F.lit(False)
        
        for c in data_cols:
            safe_col = F.coalesce(F.col(c).cast("string"), F.lit(""))
            footer_condition = footer_condition | safe_col.rlike(footer_pattern)
            non_empty_condition = non_empty_condition | (safe_col != "")
        
        # Remove footers and blank rows
        filtered_data_df = data_df.filter(non_empty_condition & ~footer_condition)
        
        # Map to requested columns and rename
        selected_columns = []
        sorted_cols = sorted(col_mapping.keys())
        for c in sorted_cols:
            original_col_name = data_cols[c]
            target_col_name = col_mapping[c]
            selected_columns.append(F.trim(F.col(original_col_name)).alias(target_col_name))
        
        selected_columns.append(F.col("_temp_row_num"))
        clean_df = filtered_data_df.select(*selected_columns)
        
    else:
        print("### -Fuzzy matcher failed or sheet is blank")
        raise_custom_error("empty_data", "Fuzzy matcher failed or sheet is blank", input_path)

    # Reorder sequentially based on original structure
    clean_df = clean_df.coalesce(1).orderBy(F.col("_temp_row_num").asc())
    
    # ===============================================================================
    # --- CONNECTION SETUP ---
    # ===============================================================================
    jdbc_url, connection_properties = get_postgres_jdbc_conf(glueContext, connection_name)

    def quote_table(raw_name):
        if "." in raw_name:
            sch, tbl = raw_name.split(".", 1)
            return f'"{sch}"."{tbl}"'
        return f'"{raw_name}"'
        
    quoted_target_table = quote_table(target_raw)

    # ===============================================================================
    # --- DBWRITE: MAIN DATA TABLE ---
    # ===============================================================================
    print(f"### -Preparing primary write to {quoted_target_table}...")
    
    # Drop the temp row num and append the necessary file metadata context
    main_write_df = clean_df.drop("_temp_row_num").withColumn(
        "filename", F.lit(input_file_name)
    ).withColumn(
        "dateread", F.current_timestamp()
    )
    
    try:
        target_schema_df = spark.read.jdbc(url=jdbc_url, table=quoted_target_table, properties=connection_properties)
        target_columns = target_schema_df.columns
        
        # Lowercase mapping for case-insensitive checks
        final_columns_lower = [c.lower() for c in main_write_df.columns]
        
        # 1. Safely add missing columns as nulls
        for t_col in target_columns:
            if row_num_label and t_col.lower() == row_num_label: 
                continue
            if t_col.lower() not in final_columns_lower:
                main_write_df = main_write_df.withColumn(t_col, F.lit(None).cast("string"))
                
        # 2. Safely alias the dataframe columns to match the target schema's exact casing
        columns_to_select = []
        for t_col in target_columns:
            if row_num_label and t_col.lower() == row_num_label: 
                continue
            # Find the actual dataframe column name regardless of case
            matched_col = next((c for c in main_write_df.columns if c.lower() == t_col.lower()), t_col)
            columns_to_select.append(F.col(matched_col).alias(t_col))
            
        main_write_df = main_write_df.select(*columns_to_select)
        
    except Exception as e:
        print(f"### -Warning: Schema alignment skipped: {str(e)}")

    try:
        main_write_df.write.option("truncate", "true").jdbc(
            url=jdbc_url, table=quoted_target_table, mode="overwrite", properties=connection_properties
        )
        print("### -SUCCESS: Primary table loaded.")
    except Exception as e:
        raise_custom_error("database_error", f"Primary write failed: {str(e)[:100]}...", input_path)