# Databricks notebook source
# MAGIC %run ./Utils

# COMMAND ----------

# Create widgets for user inputs (you only need to run these lines once)
dbutils.widgets.text("patient_id", "101", "Enter Patient ID")
dbutils.widgets.text("num_diags", "2", "Enter number of diagnoses to retrieve (leave empty for all)")
dbutils.widgets.text("num_procs", "3", "Enter number of procedures to retrieve (leave empty for all)")

# Get widget values
patient_id_input = dbutils.widgets.get("patient_id").strip().lower()
num_diags_input = dbutils.widgets.get("num_diags").strip()
num_procs_input = dbutils.widgets.get("num_procs").strip()

if patient_id_input == 'q':
    # Exit condition is not applicable in Databricks, this is just a placeholder
    print("Quitting the operation.")
else:
    try:
        patient_id = int(patient_id_input)
    except ValueError:
        print("Invalid input. Please enter a valid patient ID.")
    else:
        print(f"Patient ID: {patient_id}")

        num_diags = int(num_diags_input) if num_diags_input.isdigit() else None
        num_procs = int(num_procs_input) if num_procs_input.isdigit() else None
        
        if num_diags is not None or num_procs is not None:
            diagnoses, procedures = retrieve_patient_data(patient_id, num_diags, num_procs)

            if len(diagnoses):
                print(f"Diagnoses: {diagnoses}")
            else:
                print(f"No diagnoses found for patient {patient_id} or No diagnoses requested.")

        if len(procedures) > 0:
            print(f"Procedures: {procedures}")
        else:
            print(f"No procedures found for patient {patient_id} or No procedures requested.")
