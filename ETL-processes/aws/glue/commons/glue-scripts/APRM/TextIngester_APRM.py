import sys
import os
from pyspark.sql import functions as F
from pyspark.sql.utils import AnalysisException

# import your modular custom libraries
from CustomErrorLibs import raise_custom_error
from CustomDbConnLibs import get_postgres_jdbc_conf

# ===============================================================================
# --- MAIN PIPELINE EXECUTION ---
# ===============================================================================
def execute_pipeline(args, spark, glueContext):
    
    def get_text_arg(name, default_val):
        for i, val in enumerate(sys.argv):
            if val == f"--{name}" and len(sys.argv) > i + 1:
                return sys.argv[i + 1]
        return default_val

    delimiter_regex = get_text_arg('delimiter_regex', '\\t')
    expected_columns_raw = get_text_arg('expected_columns', None)
    filter_record_type = get_text_arg('filter_record_type', 'ALL')
    col_label = get_text_arg('col_label', 'column')
    
    row_num_label = args.get('row_number_label', '').lower()
    input_file_name = args['input_file_name']
    input_path = args['input_file_path'] + input_file_name
    connection_name = args['connection_name']
    target_raw = args['target_table']

    if not expected_columns_raw:
        raise_custom_error("missing_parameters", "Job requires --expected_columns parameter", input_path)
    
    max_cols = int(expected_columns_raw)

    # ===============================================================================
    # --- READ AND EXPAND DATA ---
    # ===============================================================================
    try:
        raw_df = spark.read.text(input_path)
    except AnalysisException as e:
        if "Path does not exist" in str(e):
            raise_custom_error("missing_file", "S3 object not found", input_path)
        else:
            raise e

    from pyspark.sql.window import Window
    from pyspark.sql.functions import row_number, lit
    window_spec = Window.orderBy(lit(1))
    raw_df = raw_df.withColumn("_temp_row_num", row_number().over(window_spec))

    split_df = raw_df.withColumn("fields", F.split(F.col("value"), delimiter_regex))

    expanded_df = split_df
    for i in range(max_cols):
        col_name = f"{col_label}{i + 1}"
        expanded_df = expanded_df.withColumn(
            col_name, F.trim(F.col("fields").getItem(i))
        ).withColumn(
            col_name, F.when(F.col(col_name) == "", F.lit(None)).otherwise(F.col(col_name))
        )

    clean_df = expanded_df.drop("value", "fields")

    if filter_record_type.upper() != 'ALL':
        clean_df = clean_df.filter(F.col(f"{col_label}1") == filter_record_type)

    ordered_columns = [f"{col_label}{i + 1}" for i in range(max_cols)]
    ordered_columns.append("_temp_row_num") 
    clean_df = clean_df.select(*ordered_columns)

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
    
    # drop the temp row num and append the necessary file metadata context
    main_write_df = clean_df.drop("_temp_row_num").withColumn(
        "filename", F.lit(input_file_name)
    ).withColumn(
        "dateread", F.current_timestamp()
    )
    
    try:
        target_schema_df = spark.read.jdbc(url=jdbc_url, table=quoted_target_table, properties=connection_properties)
        for t_col in target_schema_df.columns:
            if t_col not in main_write_df.columns:
                if row_num_label and t_col.lower() == row_num_label: continue
                main_write_df = main_write_df.withColumn(t_col, F.lit(None).cast("string"))
                
        cols_to_select = [c for c in target_schema_df.columns if not (row_num_label and c.lower() == row_num_label)]
        main_write_df = main_write_df.select(*cols_to_select)
    except Exception as e:
        print(f"### -Warning: Schema alignment skipped: {str(e)}")

    try:
        main_write_df.write.option("truncate", "true").jdbc(
            url=jdbc_url, table=quoted_target_table, mode="overwrite", properties=connection_properties
        )
        print("### -SUCCESS: Primary table loaded.")
    except Exception as e:
        raise_custom_error("database_error", f"Primary write failed: {str(e)[:100]}...", input_path)