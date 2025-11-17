# Databricks notebook source
from pyspark.sql.functions import (
    col, lit, current_date, to_date, year, month, dayofmonth, weekofyear,
    date_format, sequence, explode, expr, when, sum as spark_sum, count as spark_count, datediff
)
from pyspark.sql.types import DateType


# -----------------------
# Config
# -----------------------
silver_db = "hospital_raw_data.silver"
gold_db = "hospital_raw_data.gold"

# Ensure gold database exists
spark.sql(f"CREATE DATABASE IF NOT EXISTS {gold_db}")

# Helper: check table exists in catalog
def silver_exists(tbl_name: str) -> bool:
    full = f"{silver_db}.{tbl_name}"
    return spark.catalog.tableExists(full)

# -----------------------
# 1) DIM: dim_patient
# -----------------------
if silver_exists("patients"):
    # Keep only current SCD2 records (is_current = true). If table doesn't have is_current, fallback to all rows.
    patients = spark.table(f"{silver_db}.patients")
    if "is_current" in patients.columns:
        dim_patient = patients.filter("is_current = true").select(
            col("patient_id"),
            col("first_name"),
            col("last_name"),
            col("gender"),
            col("age"),
            col("email"),
            col("phone"),
            col("city")
        )
    else:
        dim_patient = patients.select(
            col("patient_id"),
            col("first_name"),
            col("last_name"),
            col("gender"),
            col("age"),
            col("email"),
            col("phone"),
            col("city")
        )
    dim_patient.write.format("delta").mode("overwrite").saveAsTable(f"{gold_db}.dim_patient")
    print("Created:", f"{gold_db}.dim_patient")
else:
    print("Skipping dim_patient — source silver.patients not found")

# -----------------------
# 2) DIM: dim_doctor
# -----------------------
if silver_exists("doctors"):
    doctors = spark.table(f"{silver_db}.doctors")
    if "is_current" in doctors.columns:
        dim_doctor = doctors.filter("is_current = true").select(
            col("doctor_id"),
            col("first_name"),
            col("last_name"),
            col("speciality"),
            col("experience_years")
        )
    else:
        dim_doctor = doctors.select(
            col("doctor_id"),
            col("first_name"),
            col("last_name"),
            col("speciality"),
            col("experience_years")
        )
    dim_doctor.write.format("delta").mode("overwrite").saveAsTable(f"{gold_db}.dim_doctor")
    print("Created:", f"{gold_db}.dim_doctor")
else:
    print("Skipping dim_doctor — source silver.doctors not found")

# -----------------------
# 3) DIM: dim_date (calendar)
# -----------------------
# Build date dimension from an appropriate range (earliest silver date to today if possible)
min_date = None
# try to infer earliest date from available silver tables
date_candidates = []
for t in ["admissions", "billing", "procedures", "vitals"]:
    if spark.catalog.tableExists(f"{silver_db}.{t}"):
        df = spark.table(f"{silver_db}.{t}")
        for c in ["admission_date", "billing_date", "performed_at", "timestamp", "admission_date_key"]:
            if c in df.columns:
                try:
                    mn = df.select(to_date(col(c)).alias("d")).filter(col("d").isNotNull()).agg({"d":"min"}).collect()[0][0]
                    if mn is not None:
                        date_candidates.append(mn)
                except Exception:
                    pass

if date_candidates:
    min_date = min(date_candidates)
else:
    min_date = spark.sql("select date('2020-01-01') as d").collect()[0][0]  # fallback

# make sure min_date is a string date
if isinstance(min_date, str):
    start_date = min_date
else:
    start_date = min_date.strftime("%Y-%m-%d")

end_date = spark.sql("select current_date() as d").collect()[0][0].strftime("%Y-%m-%d")

date_df = spark.range(1).select(
    explode(sequence(to_date(lit(start_date)), to_date(lit(end_date)))).alias("date")
)
dim_date = date_df.withColumn("year", year("date")) \
    .withColumn("month", month("date")) \
    .withColumn("day", dayofmonth("date")) \
    .withColumn("week", weekofyear("date")) \
    .withColumn("weekday", date_format("date", "EEE")) \
    .withColumn("month_name", date_format("date", "MMMM"))

dim_date.write.format("delta").mode("overwrite").saveAsTable(f"{gold_db}.dim_date")
print("Created:", f"{gold_db}.dim_date (range: {start_date}..{end_date})")

# -----------------------
# 4) FACT: fact_billing
# -----------------------
if silver_exists("billing"):
    billing = spark.table(f"{silver_db}.billing")
    # if SCD present, keep only current
    if "is_current" in billing.columns:
        billing = billing.filter("is_current = true")
    # ensure columns: billing_id, billing_date, patient_id, service, amount
    required = set(["billing_id", "billing_date", "patient_id", "service", "amount"])
    # if your silver.billing consolidates line_items into single rows, adapt accordingly
    # The script assumes billing has one row per line item (service, amount)
    available = set(billing.columns)
    missing = required - available
    if missing:
        print("fact_billing: missing columns in silver.billing:", missing, "- adapt source or update pipeline.")
    else:
        fact_billing = billing.select(
            col("billing_id"),
            col("patient_id"),
            col("service"),
            col("amount"),
            to_date(col("billing_date")).alias("billing_date")
        ).withColumn("billing_year", year(col("billing_date"))) \
         .withColumn("billing_month", month(col("billing_date")))
        # partition by year/month for performance
        fact_billing.write.format("delta").mode("overwrite") \
            .option("overwriteSchema", "true") \
            .partitionBy("billing_year", "billing_month") \
            .saveAsTable(f"{gold_db}.fact_billing")
        print("Created:", f"{gold_db}.fact_billing")
else:
    print("Skipping fact_billing — source silver.billing not found")

# -----------------------
# 5) FACT: fact_admissions
# -----------------------
if silver_exists("admissions"):
    admissions = spark.table(f"{silver_db}.admissions")
    if "is_current" in admissions.columns:
        admissions = admissions.filter("is_current = true OR is_current IS NULL")  # allow older schemas
    # Ensure date types
    admissions = admissions.withColumn("admission_date", to_date(col("admission_date"))) \
                           .withColumn("discharge_date", to_date(col("discharge_date")))
    # Calculate length_of_stay when discharge available
    fact_admissions = admissions.select(
        col("admission_id"),
        col("patient_id"),
        col("admission_date"),
        col("discharge_date"),
        col("reason"),
        col("room_no")
    ).withColumn("length_of_stay_days", when(col("discharge_date").isNotNull(), datediff(col("discharge_date"), col("admission_date"))).otherwise(None)) \
     .withColumn("admission_year", year(col("admission_date"))) \
     .withColumn("admission_month", month(col("admission_date")))

    fact_admissions.write.format("delta").mode("overwrite") \
        .option("overwriteSchema", "true") \
        .partitionBy("admission_year", "admission_month") \
        .saveAsTable(f"{gold_db}.fact_admissions")
    print("Created:", f"{gold_db}.fact_admissions")
else:
    print("Skipping fact_admissions — source silver.admissions not found")

# -----------------------
# 6) FACT: fact_vitals
# -----------------------
if silver_exists("vitals"):
    vitals = spark.table(f"{silver_db}.vitals")
    # choose current SCD rows if relevant (vitals usually append-only)
    # Normalize timestamp and date
    ts_col = None
    if "timestamp" in vitals.columns:
        ts_col = "timestamp"
    elif "vital_timestamp" in vitals.columns:
        ts_col = "vital_timestamp"
    elif "vital_date" in vitals.columns:
        ts_col = "vital_date"

    if ts_col is None:
        print("fact_vitals: no timestamp column found in silver.vitals. Expected 'timestamp' or similar.")
    else:
        fact_vitals = vitals.withColumn("vital_ts", col(ts_col).cast("timestamp")) \
            .withColumn("vital_date", to_date(col("vital_ts"))) \
            .select(
                col("vital_id"),
                col("patient_id"),
                col("vital_ts").alias("timestamp"),
                col("vital_date"),
                col("heart_rate"),
                col("temperature")
            ).withColumn("vital_year", year(col("vital_date"))) \
             .withColumn("vital_month", month(col("vital_date")))

        fact_vitals.write.format("delta").mode("overwrite") \
            .option("overwriteSchema", "true") \
            .partitionBy("vital_year", "vital_month") \
            .saveAsTable(f"{gold_db}.fact_vitals")
        print("Created:", f"{gold_db}.fact_vitals")
else:
    print("Skipping fact_vitals — source silver.vitals not found")

# -----------------------
# 7) Optional: fact_doctor_activity (from procedures or admissions if doctor_id present)
# -----------------------
# We prefer procedures table (procedures often have performed_at and patient_id + maybe doctor_id)
if silver_exists("procedures"):
    procedures = spark.table(f"{silver_db}.procedures")
    # check if doctor_id exists in procedures, else try admissions
    if "doctor_id" in procedures.columns:
        fact_doctor_activity = procedures.select(
            col("procedure_id"),
            col("doctor_id"),
            col("patient_id"),
            to_date(col("performed_at")).alias("activity_date"),
            col("procedure_name"),
            col("cost")
        ).withColumn("activity_year", year(col("activity_date"))) \
         .withColumn("activity_month", month(col("activity_date")))
        fact_doctor_activity.write.format("delta").mode("overwrite") \
            .option("overwriteSchema", "true") \
            .partitionBy("activity_year", "activity_month") \
            .saveAsTable(f"{gold_db}.fact_doctor_activity")
        print("Created:", f"{gold_db}.fact_doctor_activity")
    else:
        print("procedures exists but doctor_id column not present — skipping fact_doctor_activity from procedures.")
else:
    print("Skipping fact_doctor_activity — source silver.procedures not found")

# -----------------------
# 8) Useful aggregated summary tables
# -----------------------
# Revenue by month
if spark.catalog.tableExists(f"{gold_db}.fact_billing"):
    spark.sql(f"""
        CREATE OR REPLACE TABLE {gold_db}.agg_revenue_month
        USING DELTA AS
        SELECT
            billing_year,
            billing_month,
            SUM(amount) AS total_revenue,
            COUNT(*) AS lines_count
        FROM {gold_db}.fact_billing
        GROUP BY billing_year, billing_month
    """)
    print("Created aggregate:", f"{gold_db}.agg_revenue_month")

# Admissions by reason per month
if spark.catalog.tableExists(f"{gold_db}.fact_admissions"):
    spark.sql(f"""
        CREATE OR REPLACE TABLE {gold_db}.agg_admissions_reason_month
        USING DELTA AS
        SELECT
            admission_year,
            admission_month,
            reason,
            COUNT(*) AS admissions_count,
            AVG(length_of_stay_days) AS avg_los_days
        FROM {gold_db}.fact_admissions
        GROUP BY admission_year, admission_month, reason
    """)
    print("Created aggregate:", f"{gold_db}.agg_admissions_reason_month")

print("Gold layer build finished.")
