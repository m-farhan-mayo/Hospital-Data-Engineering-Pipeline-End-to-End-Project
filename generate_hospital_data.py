import os
import csv
import json
import uuid
import random
from faker import Faker

fake = Faker()

OUTPUT_DIR = "hospital_raw_data"

if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)

print(f"Saving generated data to: {OUTPUT_DIR}/")

# -----------------------------
# Helper: High duplication & errors
# -----------------------------
def maybe_duplicate(value, chance=0.3):
    """Return value or duplicate of previous"""
    if random.random() < chance:
        return value
    return value

def maybe_none_or_err(value, err_rate=0.2, none_rate=0.2, err_str="ERR"):
    """Return value, None, or error string. Convert dates to ISO string."""
    r = random.random()
    if r < err_rate:
        return err_str
    elif r < err_rate + none_rate:
        return None
    else:
        if hasattr(value, "isoformat"):  # date or datetime
            return value.isoformat()
        return value

# -----------------------------
# 1. Generate patients
# -----------------------------
def generate_patients_csv(n=1000):
    filepath = os.path.join(OUTPUT_DIR, "patients_raw.csv")
    patients = []

    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["patient_id", "first_name", "last_name", "gender", "age", "email", "phone", "city"])

        for _ in range(n):
            patient_id = str(uuid.uuid4())
            first_name = maybe_duplicate(fake.first_name(), 0.3)
            last_name = maybe_duplicate(fake.last_name(), 0.3)
            gender = random.choice(["M", "F", "O", None])
            age = maybe_none_or_err(fake.random_int(1, 100))
            email = fake.email() if random.random() > 0.1 else ""
            phone = maybe_none_or_err(fake.phone_number())
            city = maybe_duplicate(fake.city(), 0.3)

            writer.writerow([patient_id, first_name, last_name, gender, age, email, phone, city])
            patients.append(patient_id)

            # Random duplicate row
            if random.random() < 0.05:
                writer.writerow([patient_id, first_name, last_name, gender, age, email, phone, city])
                patients.append(patient_id)

    print("Generated patients_raw.csv")
    return patients

# -----------------------------
# 2. Generate doctors
# -----------------------------
def generate_doctors_csv(n=200):
    filepath = os.path.join(OUTPUT_DIR, "doctors_raw.csv")
    doctors = []

    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["doctor_id", "first_name", "last_name", "speciality", "experience_years"])

        for _ in range(n):
            doctor_id = str(uuid.uuid4())
            first_name = maybe_duplicate(fake.first_name(), 0.3)
            last_name = maybe_duplicate(fake.last_name(), 0.3)
            speciality = random.choice(["Cardiology", "Orthopedics", "Neurology", "Oncology", "General Medicine"])
            experience_years = maybe_none_or_err(fake.random_int(1, 40), err_str="EXP_ERR")

            writer.writerow([doctor_id, first_name, last_name, speciality, experience_years])
            doctors.append(doctor_id)

            # Random duplicate row
            if random.random() < 0.05:
                writer.writerow([doctor_id, first_name, last_name, speciality, experience_years])
                doctors.append(doctor_id)

    print("Generated doctors_raw.csv")
    return doctors

# -----------------------------
# 3. Generate admissions
# -----------------------------
def generate_admissions_csv(patients, doctors, n=2000):
    filepath = os.path.join(OUTPUT_DIR, "admissions_raw.csv")

    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["admission_id", "patient_id", "admission_date", "discharge_date", "reason", "room_no", "doctor_id"])

        for _ in range(n):
            admission_id = str(uuid.uuid4())
            patient_id = maybe_duplicate(random.choice(patients), 0.3)
            doctor_id = maybe_duplicate(random.choice(doctors), 0.2)
            admission_date = maybe_none_or_err(fake.date_this_year())
            discharge_date = maybe_none_or_err(fake.date_this_year())
            reason = random.choice(["Fever", "Injury", "Fracture", "Cancer", "Asthma"])
            room_no = maybe_none_or_err(fake.random_int(100, 500))

            writer.writerow([admission_id, patient_id, admission_date, discharge_date, reason, room_no, doctor_id])

            # Duplicate some admissions
            if random.random() < 0.1:
                writer.writerow([admission_id, patient_id, admission_date, discharge_date, reason, room_no, doctor_id])

    print("Generated admissions_raw.csv")

# -----------------------------
# 4. Generate vitals
# -----------------------------
def generate_vitals_json(patients, n=2000):
    filepath = os.path.join(OUTPUT_DIR, "vitals_raw.json")

    with open(filepath, "w", encoding="utf-8") as f:
        for _ in range(n):
            record = {
                "vital_id": str(uuid.uuid4()),
                "patient_id": maybe_duplicate(random.choice(patients), 0.3),
                "temperature": maybe_none_or_err(round(random.uniform(96, 104), 1), err_str="TEMP_ERR"),
                "heart_rate": maybe_none_or_err(fake.random_int(50, 150)),
                "timestamp": maybe_none_or_err(fake.date_time_this_year())
            }
            f.write(json.dumps(record) + "\n")

            # Duplicate some vitals
            if random.random() < 0.1:
                f.write(json.dumps(record) + "\n")

    print("Generated vitals_raw.json")

# -----------------------------
# 5. Generate procedures
# -----------------------------
def generate_procedures_json(patients, n=2000):
    filepath = os.path.join(OUTPUT_DIR, "procedures_raw.json")

    with open(filepath, "w", encoding="utf-8") as f:
        for _ in range(n):
            record = {
                "procedure_id": str(uuid.uuid4()),
                "patient_id": maybe_duplicate(random.choice(patients), 0.3),
                "procedure_name": random.choice(["X-Ray", "MRI", "CT Scan", "Blood Test", "Surgery"]),
                "cost": maybe_none_or_err(round(random.uniform(50, 5000), 2), err_str="COST_ERR"),
                "performed_at": maybe_none_or_err(fake.date_time_this_year())
            }
            f.write(json.dumps(record) + "\n")

            # Duplicate some procedures
            if random.random() < 0.1:
                f.write(json.dumps(record) + "\n")

    print("Generated procedures_raw.json")

# -----------------------------
# 6. Generate billing
# -----------------------------
def generate_billing_json(patients, n=2000):
    filepath = os.path.join(OUTPUT_DIR, "billing_raw.json")

    with open(filepath, "w", encoding="utf-8") as f:
        for _ in range(n):
            record = {
                "billing_id": str(uuid.uuid4()),
                "patient_id": maybe_duplicate(random.choice(patients), 0.3),
                "line_items": [
                    {
                        "service": random.choice(["Consultation", "Surgery", "Medication", "ICU"]),
                        "amount": maybe_none_or_err(round(random.uniform(20, 7000), 2), err_str="BAD_AMOUNT")
                    }
                    for __ in range(random.randint(1, 4))
                ],
                "total": maybe_none_or_err(round(random.uniform(100, 20000), 2)),
                "billing_date": maybe_none_or_err(fake.date_this_year())
            }
            f.write(json.dumps(record) + "\n")

            # Duplicate some billing
            if random.random() < 0.1:
                f.write(json.dumps(record) + "\n")

    print("Generated billing_raw.json")

# ----------------------------------
# RUN ALL GENERATORS
# ----------------------------------
patients = generate_patients_csv()
doctors = generate_doctors_csv()
generate_admissions_csv(patients, doctors)
generate_vitals_json(patients)
generate_procedures_json(patients)
generate_billing_json(patients)

print("\nAll data generated successfully!")
