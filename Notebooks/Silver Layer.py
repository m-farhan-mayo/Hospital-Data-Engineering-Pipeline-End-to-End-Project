# Databricks notebook source
from typing import List
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %md
# MAGIC ## `Admission Transformation`

# COMMAND ----------

df_admission = spark.read.csv(
    '/Volumes/hospital_raw_data/bronze/rawdata/admissions_raw.csv',
    header=True,
    inferSchema=True
)

# COMMAND ----------

# -------------------------------
# Step 1: Clean the data
# -------------------------------
df_admission = (
    df_admission
    .withColumn(
        "admission_date_clean",
        when(
            (col("admission_date").isNull()) |
            (col("admission_date").isin("-", "null", "ERR")),
            None
        ).otherwise(to_date(col("admission_date")))
    )
    .withColumn(
        "discharge_date_clean",
        when(
            (col("discharge_date").isNull()) |
            (col("discharge_date").isin("-", "null", "ERR")),
            None
        ).otherwise(to_date(col("discharge_date")))
    )
    .withColumn(
        "discharge_date_final",
        when(col("discharge_date_clean") < col("admission_date_clean"), None)
        .otherwise(col("discharge_date_clean"))
    )
    .withColumn("reason_clean", initcap(trim(col("reason"))))
    .withColumn(
        "room_no_clean",
        when(
            (col("room_no").isNull()) | (col("room_no").isin("-", "null", "ERR")),
            None
        ).otherwise(col("room_no").cast("int"))
    )
)

# -------------------------------
# Step 2: Select final columns & drop rows with null essential IDs
# -------------------------------
df_admission = df_admission.select(
    "admission_id",
    "patient_id",
    col("admission_date_clean").alias("admission_date"),
    col("discharge_date_final").alias("discharge_date"),
    col("reason_clean").alias("reason"),
    col("room_no_clean").alias("room_no")
).dropna(subset=["admission_id", "patient_id", "admission_date"])

# -------------------------------
# Step 3: Add SCD2 Metadata
# -------------------------------
df_admission = df_admission.withColumn("effective_date", current_date()) \
                      .withColumn("end_date", lit(None).cast("date")) \
                      .withColumn("is_current", lit(True))

# -------------------------------
# Step 4: Apply SCD2 Merge
# -------------------------------
table_name = "hospital_raw_data.silver.admissions"

if spark.catalog.tableExists(table_name):
    deltaTable = DeltaTable.forName(spark, table_name)
    
    (deltaTable.alias("old")
        .merge(
            df_admission.alias("new"),
            "old.admission_id = new.admission_id AND old.is_current = true"
        )
        .whenMatchedUpdate(
            condition="""
                old.patient_id != new.patient_id OR
                old.admission_date != new.admission_date OR
                old.discharge_date != new.discharge_date OR
                old.reason != new.reason OR
                old.room_no != new.room_no
            """,
            set={
                "end_date": current_date(),
                "is_current": lit(False)
            }
        )
        .whenNotMatchedInsert(
            values={
                "admission_id": col("new.admission_id"),
                "patient_id": col("new.patient_id"),
                "admission_date": col("new.admission_date"),
                "discharge_date": col("new.discharge_date"),
                "reason": col("new.reason"),
                "room_no": col("new.room_no"),
                "effective_date": col("new.effective_date"),
                "end_date": col("new.end_date"),
                "is_current": col("new.is_current")
            }
        )
        .execute()
    )
else:
    df_admission.write.format("delta").mode("overwrite").saveAsTable(table_name)

# COMMAND ----------

# Count total rows in df_scd2 (cleaned admissions DataFrame)
total_rows = df_admission.count()
print(f"Total rows: {total_rows}")


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM hospital_raw_data.silver.admissions

# COMMAND ----------

# MAGIC %md
# MAGIC ## `Billing Transformation`

# COMMAND ----------

df_billing = spark.read.json(
    '/Volumes/hospital_raw_data/bronze/rawdata/billing_raw.json',
    multiLine=True
)

# COMMAND ----------

# -------------------------------
# Step 1: Flatten line_items array
# -------------------------------
df_billing = df_billing.withColumn("line_item", explode("line_items"))

# -------------------------------
# Step 2: Extract service and amount
# -------------------------------
df_billing = df_billing.withColumn("service", col("line_item.service")) \
                           .withColumn("amount", col("line_item.amount"))

# -------------------------------
# Step 3: Select final flattened columns
# -------------------------------
df_billing = df_billing.select(
    "billing_date",
    "billing_id",
    "service",
    "amount"
)

# COMMAND ----------

from pyspark.sql.functions import col, when, lit, to_date, current_date, regexp_replace
from delta.tables import DeltaTable # Required for Delta Lake merge operations

# --- Deduplication ---
df_billing = df_billing.dropDuplicates(["billing_id", "service", "amount", "billing_date"])

# --- Cleaning for SCD ---

# 1. Clean 'amount' column: The most robust approach to handle non-numeric inputs
#    when 'try_cast' is unavailable and the user wants errors treated as 0.

# STEP 1A: Apply cleaning to replace all non-numeric characters (except decimal and minus sign).
# This results in a column that is either a clean number string (e.g., "123.45") or an empty string ("").
cleaned_amount_str = regexp_replace(col("amount"), "[^0-9\\.\\-]", "")

# STEP 1B: Use a WHEN statement to explicitly check for the original NULLs, and the resulting
# empty string from the cleanup, and set those values to 0.0 before casting the rest.
df_billing = df_billing.withColumn(
    "amount",
    when(
        col("amount").isNull() | # Handle original nulls
        (cleaned_amount_str == ""), # CRITICAL: Explicitly check for empty string result ('') and replace it with 0.0
        lit(0.0) 
    ).otherwise(
        # Cast the guaranteed non-empty/numeric-looking string to double.
        cleaned_amount_str.cast("double")
    )
)


# 2. Clean 'billing_date' column: Replace bad strings with null, then apply to_date() conditionally.
df_billing = df_billing.withColumn(
    "billing_date",
    when(
        (col("billing_date").isNull()) |
        (col("billing_date") == "ERR") |
        (col("billing_date") == "-"),
        lit(None).cast("date") # Use lit(None) and explicitly cast the null to date type
    ).otherwise(to_date("billing_date"))
)

# --- Drop rows with null essential columns ---
# NOTE: Since we replaced all bad amounts with 0.0, this dropna only affects rows where date, service, or ID are null.
df_billing = df_billing.dropna(subset=["billing_id", "service", "billing_date", "amount"])

# --- Add SCD2 Metadata (Rest of your code remains the same) ---
df_billing = df_billing.withColumn("effective_date", current_date()) \
                     .withColumn("end_date", lit(None).cast("date")) \
                     .withColumn("is_current", lit(True))

# --- Apply SCD2 ---
table_name = "hospital_raw_data.silver.billing"

if spark.catalog.tableExists(table_name):
    deltaTable = DeltaTable.forName(spark, table_name)
    
    (deltaTable.alias("old")
        .merge(
            df_billing.alias("new"),
            """
            old.billing_id = new.billing_id 
            AND old.service = new.service 
            AND old.is_current = true
            """
        )
        # Expire old record if amount or billing_date changed
        .whenMatchedUpdate(
            condition="old.amount != new.amount OR old.billing_date != new.billing_date",
            set={
                "end_date": current_date(),
                "is_current": lit(False)
            }
        )
        # Insert new record for changed or new rows
        .whenNotMatchedInsert(
            values={
                "billing_id": col("new.billing_id"),
                "billing_date": col("new.billing_date"),
                "service": col("new.service"),
                "amount": col("new.amount"),
                "effective_date": col("new.effective_date"),
                "end_date": col("new.end_date"),
                "is_current": col("new.is_current")
            }
        )
        .execute()
    )
else:
    # First-time creation
    df_billing.write \
        .format("delta") \
        .mode("overwrite") \
        .saveAsTable(table_name)

# COMMAND ----------

# Count total rows in df_scd2 (cleaned admissions DataFrame)
total_rows = df_billing.count()
print(f"Total rows: {total_rows}")


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM hospital_raw_data.silver.billing

# COMMAND ----------

# MAGIC %md
# MAGIC ## `Doctor Transformation`

# COMMAND ----------

df_doctor = spark.read.csv(
    '/Volumes/hospital_raw_data/bronze/rawdata/doctors_raw.csv',
    header=True,
    inferSchema=True
)

# COMMAND ----------

display(df_doctor)

# COMMAND ----------

# --- Clean experience_years ---
df_doctor = df_doctor.withColumn(
    "experience_years_clean",
    when(
        (col("experience_years").isNull()) | (col("experience_years") == "EXP_ERR"),
        None
    ).otherwise(col("experience_years").cast("int"))
)

# --- Drop rows with null doctor_id or experience_years_clean ---
df_doctor = df_doctor.dropna(subset=["doctor_id", "experience_years_clean"])

# --- Deduplication ---
df_doctor = df_doctor.dropDuplicates([
    "doctor_id", "first_name", "last_name", "speciality", "experience_years_clean"
])

# --- Add SCD2 Metadata ---
df_doctor = df_doctor.withColumn("effective_date", current_date()) \
                                   .withColumn("end_date", lit(None).cast("date")) \
                                   .withColumn("is_current", lit(True))

# --- Apply SCD2 ---
table_name = "hospital_raw_data.silver.doctors"

if spark.catalog.tableExists(table_name):
    deltaTable = DeltaTable.forName(spark, table_name)
    
    (deltaTable.alias("old")
        .merge(
            df_doctor.alias("new"),
            """
            old.doctor_id = new.doctor_id
            AND old.is_current = true
            """
        )
        # Expire old record if any relevant field changed
        .whenMatchedUpdate(
            condition="""
                old.first_name != new.first_name OR
                old.last_name != new.last_name OR
                old.speciality != new.speciality OR
                old.experience_years != new.experience_years_clean
            """,
            set={
                "end_date": current_date(),
                "is_current": lit(False)
            }
        )
        # Insert new record for changed or new rows
        .whenNotMatchedInsert(
            values={
                "doctor_id": col("new.doctor_id"),
                "first_name": col("new.first_name"),
                "last_name": col("new.last_name"),
                "speciality": col("new.speciality"),
                "experience_years": col("new.experience_years_clean"),
                "effective_date": col("new.effective_date"),
                "end_date": col("new.end_date"),
                "is_current": col("new.is_current")
            }
        )
        .execute()
    )
else:
    # First-time creation
    df_doctor.write \
        .format("delta") \
        .mode("overwrite") \
        .saveAsTable(table_name)

# COMMAND ----------

# Count total rows in df_scd2 (cleaned admissions DataFrame)
total_rows = df_doctor.count()
print(f"Total rows: {total_rows}")


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM hospital_raw_data.silver.doctors

# COMMAND ----------

# MAGIC %md
# MAGIC ## `Patient Transformation`

# COMMAND ----------

df_patient = spark.read.csv(
    '/Volumes/hospital_raw_data/bronze/rawdata/patients_raw.csv',
    header=True,
    inferSchema=True
)

# COMMAND ----------

display(df_patient)

# COMMAND ----------

# --- Clean age ---
df_patient = df_patient.withColumn(
    "age_clean",
    when((col("age").isNull()) | (col("age") == "ERR"), None)
    .otherwise(col("age").cast("int"))
)

# --- Clean gender ---
df_patient = df_patient.withColumn(
    "gender_clean",
    when(col("gender").isin("M", "F", "O"), col("gender"))
    .otherwise(None)
)

# --- Clean phone ---
df_patient = df_patient.withColumn(
    "phone",
    regexp_replace(col("phone"), "[^0-9]", "")  # Keep only digits
)

# --- Drop rows with null patient_id, age, or gender ---
df_patient = df_patient.dropna(subset=["patient_id", "age_clean", "gender_clean"])

# --- Deduplicate ---
df_patient = df_patient.dropDuplicates([
    "patient_id", "first_name", "last_name", "gender_clean", "age_clean", "email", "city", "phone"
])

# --- Add SCD2 fields ---
df_patient = df_patient.withColumn("effective_date", current_date()) \
                                    .withColumn("end_date", lit(None).cast("date")) \
                                    .withColumn("is_current", lit(True))

# --- Delta Merge (SCD Type 2) ---
table_name = "hospital_raw_data.silver.patients"

if spark.catalog.tableExists(table_name):
    deltaTable = DeltaTable.forName(spark, table_name)
    
    (deltaTable.alias("old")
        .merge(
            df_patient.alias("new"),
            "old.patient_id = new.patient_id AND old.is_current = true"
        )
        .whenMatchedUpdate(
            condition="""
                old.first_name != new.first_name OR
                old.last_name != new.last_name OR
                old.gender != new.gender_clean OR
                old.age != new.age_clean OR
                old.email != new.email OR
                old.phone != new.phone OR
                old.city != new.city
            """,
            set={
                "end_date": current_date(),
                "is_current": lit(False)
            }
        )
        .whenNotMatchedInsert(
            values={
                "patient_id": col("new.patient_id"),
                "first_name": col("new.first_name"),
                "last_name": col("new.last_name"),
                "gender": col("new.gender_clean"),
                "age": col("new.age_clean"),
                "email": col("new.email"),
                "phone": col("new.phone"),
                "city": col("new.city"),
                "effective_date": col("new.effective_date"),
                "end_date": col("new.end_date"),
                "is_current": col("new.is_current")
            }
        )
        .execute()
    )
else:
    df_patient.write.format("delta").mode("overwrite").saveAsTable(table_name)

# COMMAND ----------

# Count total rows in df_scd2 (cleaned admissions DataFrame)
total_rows = df_patient.count()
print(f"Total rows: {total_rows}")


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM hospital_raw_data.silver.patients

# COMMAND ----------

# MAGIC %md
# MAGIC ## `Procedures Transformation`

# COMMAND ----------

df_procedures = spark.read.json(
    '/Volumes/hospital_raw_data/bronze/rawdata/procedures_raw.json',
)

# COMMAND ----------

display(df_procedures.limit(20))

# COMMAND ----------

# --- Cleaning Cost ---
df_procedures = df_procedures.withColumn(
    "cost_clean",
    when((col("cost").isNull()) | (col("cost") == "COST_ERR"), None)
    .otherwise(col("cost").cast("double"))
)

# --- Cleaning ID ---
df_procedures = df_procedures.withColumn(
    "patient_id_clean",
    when(col("patient_id") == "ERR", None).otherwise(col("patient_id"))
)

# --- Drop rows with null patient_id or null cost ---
df_procedures = df_procedures.dropna(subset=["patient_id_clean", "cost_clean"])

# --- Deduplication ---
df_procedures = df_procedures.dropDuplicates([
    "patient_id_clean", "performed_at", "procedure_id", "procedure_name", "cost_clean"
])

# --- SCD MetaData ---
df_procedures = df_procedures.withColumn("effective_date", current_date()) \
                                        .withColumn("end_date", lit(None).cast("date")) \
                                        .withColumn("is_current", lit(True))

# --- Applying SCD ---
table_name = "hospital_raw_data.silver.procedures"

if spark.catalog.tableExists(table_name):
    deltaTable = DeltaTable.forName(spark, table_name)
    
    (deltaTable.alias("old")
        .merge(
            df_procedures.alias("new"),
            "old.procedure_id = new.procedure_id AND old.is_current = true"
        )
        .whenMatchedUpdate(
            condition="""
                old.patient_id_clean != new.patient_id_clean OR
                old.performed_at != new.performed_at OR
                old.procedure_name != new.procedure_name OR
                old.cost_clean != new.cost_clean
            """,
            set={
                "end_date": current_date(),
                "is_current": lit(False)
            }
        )
        .whenNotMatchedInsertAll()
        .execute()
    )
else:
    df_procedures.write.format("delta").mode("overwrite").saveAsTable(table_name)

# COMMAND ----------

# Count total rows in df_scd2 (cleaned admissions DataFrame)
total_rows = df_procedures.count()
print(f"Total rows: {total_rows}")


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM hospital_raw_data.silver.procedures

# COMMAND ----------

# MAGIC %md
# MAGIC ## `vital Transformation`

# COMMAND ----------

df_vital = spark.read.json(
    '/Volumes/hospital_raw_data/bronze/rawdata/vitals_raw.json'
)

# COMMAND ----------

display(df_vital.limit(20))

# COMMAND ----------

df_vital = df_vital.withColumn(
    "heart_rate_clean",
    when((col("heart_rate").isNull()) | (col("heart_rate") == "ERR"), None)
    .otherwise(col("heart_rate").cast("int"))
)

# Clean ID

df_vital = df_vital.withColumn(
    "patient_id_clean",
    when((col("patient_id").isNull()) | (col("patient_id") == "TEMP_ERR"), None)
    .otherwise(col("patient_id"))
)

# Cleaning TimeStamp

df_vital = df_vital.withColumn(
    "timestamp_clean",
    when(col("timestamp").isin(["ERR", None]), None)
    .otherwise(to_timestamp(col("timestamp")))
)

# Removing Duplicates

df_vital = df_vital.dropDuplicates([
    "vital_id", "patient_id_clean", "timestamp_clean", "heart_rate_clean", "temperature"
])

df_vital = df_vital.dropna(subset=["patient_id_clean", "heart_rate_clean", "timestamp_clean", "temperature"])


# --- SCD MetaData
df_vital = df_vital.withColumn("effective_date", current_date()) \
                                  .withColumn("end_date", lit(None).cast("date")) \
                                  .withColumn("is_current", lit(True))

# --- Target table ---
table_name = "hospital_raw_data.silver.vitals"

# --- Apply SCD ---
if spark.catalog.tableExists(table_name):
    deltaTable = DeltaTable.forName(spark, table_name)
    
    (deltaTable.alias("old")
        .merge(
            df_vital.alias("new"),
            "old.vital_id = new.vital_id AND old.is_current = true"
        )
        .whenMatchedUpdate(
            condition="""
                old.heart_rate != new.heart_rate OR
                old.temperature != new.temperature OR
                old.timestamp != new.timestamp
            """,
            set={
                "end_date": current_date(),
                "is_current": lit(False)
            }
        )
        .whenNotMatchedInsertAll()
        .execute()
    )
else:
    df_vital.write.format("delta").mode("overwrite").saveAsTable(table_name)


# COMMAND ----------

# Count total rows in df_scd2 (cleaned admissions DataFrame)
total_rows = df_vital.count()
print(f"Total rows: {total_rows}")


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM hospital_raw_data.silver.vitals