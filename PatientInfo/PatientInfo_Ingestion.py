# Databricks notebook source
# MAGIC %run ./Utils

# COMMAND ----------

file_path = "/Volumes/dev_catalog/bronze/patientinfo/main_table.txt"
df = read_file(file_path, file_format='csv', options={"header": True, "sep": "|"})
df.display()

# COMMAND ----------

# df_processed = df.withColumn("dx_code", split(regexp_replace(col("dx_code"), r"\[|'\]", ""), ",")) \
#                 .withColumn("px_code", split(regexp_replace(col("px_code"), r"\[|'\]", ""), ",")) \
#                 .withColumn("dx_code", explode(col("dx_code"))) \

# df_processed.display()

# COMMAND ----------

# Explode the diagnosis codes into individual rows

processed_df = df.withColumn("dx_code", split(col("dx_code"), "','")) \
                .withColumn("px_code", split(col("px_code"), "','"))

diagnostics_df = processed_df \
    .withColumn("dx_code", explode(col("dx_code"))) \
    .withColumn("dx_code", regexp_replace(col("dx_code"), r"\['|'\]", "")) \
    .select("patient_id", "dx_code") \
    .filter(col("dx_code").isNotNull())

diagnostics_df.display()

# Explode the procedure codes into individual rows
procedures_df = processed_df \
    .withColumn("px_code", explode(col("px_code"))) \
    .withColumn("px_code", regexp_replace(col("px_code"), r"\['|'\]", "")) \
    .select("patient_id", "px_code") \
    .filter(col("px_code").isNotNull())

processed_df.display()

# COMMAND ----------

# Define table names
diagnostics_table_name = "dev_catalog.silver.diagnostics"
procedures_table_name = "dev_catalog.silver.procedures"

# Merge condition for diagnostics
diagnostics_merge_condition = "target.patient_id = source.patient_id AND target.dx_code = source.dx_code"

# Merge condition for procedures
procedures_merge_condition = "target.patient_id = source.patient_id AND target.px_code = source.px_code"

# Upsert the diagnostics data
upsert_to_delta(diagnostics_df, diagnostics_table_name, diagnostics_merge_condition)

# Upsert the procedures data
upsert_to_delta(procedures_df, procedures_table_name, procedures_merge_condition)
