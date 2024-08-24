# Databricks notebook source
def read_file(file_path, file_format='csv', options=None):
    # Default options
    if options is None:
        options = {}
    
    # Read file using the supplied file_format and the options
    df = (spark.read.format(file_format)
                 .options(**options)
                 .load(file_path))
    
    return df

# COMMAND ----------

def write_file(df, output_path, file_format='csv', options=None):
    # Default options
    if options is None:
        options = {}

    # Write file using the supplied file_format and the options
    df.write.format(file_format).options(**options).save(output_path)

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql.utils import AnalysisException

def write_to_delta(df, tbl_fqdn, options=None):
    # Default options
    if options is None:
        options = {}

    # Write as delta table
    df.write.format('delta').options(**options).saveAsTable(tbl_fqdn)

def upsert_to_delta(df, tbl_name, merge_condition):
    # Attempt to load the Delta table
    try:
        delta_table = DeltaTable.forName(spark, tbl_name)
        
        # Perform the merge operation if the table exists
        delta_table.alias("target").merge(
            df.alias("source"),
            merge_condition
        ).whenNotMatchedInsertAll().execute()

    except AnalysisException as e:
        if "DELTA_MISSING_DELTA_TABLE" in str(e):
            # If the table does not exist, create it and then perform the merge operation
            write_to_delta(df, tbl_name)
            # Reload the Delta table after creation
            delta_table = DeltaTable.forName(spark, tbl_name)
            delta_table.alias("target").merge(
                df.alias("source"),
                merge_condition
            ).whenNotMatchedInsertAll().execute()
        else:
            # If there's a different error, re-raise it
            raise e


# COMMAND ----------

from pyspark.sql.functions import *

def parse_array(column):
    return expr(f"CASE WHEN {column} IS NULL OR {column} = '' THEN ARRAY() ELSE split(regexp_replace({column}, '[\\[\\]'' ]', ''), ',') END")

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def retrieve_patient_data(patient_id, num_diags=None, num_procs=None):
    diagnostics_table_name = "dev_catalog.silver.diagnostics"
    procedures_table_name = "dev_catalog.silver.procedures"

    # Load the data from Delta tables
    diagnoses_df = spark.table(diagnostics_table_name).filter(col("patient_id") == patient_id)
    procedures_df = spark.table(procedures_table_name).filter(col("patient_id") == patient_id)

    # Apply additional filtering based on num_diags and num_procs
    if num_diags is not None:
        diagnoses_filtered_df = diagnoses_df.limit(num_diags)
    if num_procs is not None:
        procedures_filtered_df = procedures_df.limit(num_procs)

    # Collect results
    diagnoses = diagnoses_filtered_df.collect()
    procedures = procedures_filtered_df.collect()

    # Convert to lists or another suitable format for easy output
    diagnoses_list = [row.asDict() for row in diagnoses]
    procedures_list = [row.asDict() for row in procedures]

    return diagnoses_list, procedures_list


