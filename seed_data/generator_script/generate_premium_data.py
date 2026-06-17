import pandas as pd
import random
from faker import Faker
from datetime import datetime

fake = Faker()

START_YEAR = 2015
END_YEAR = 2025

INITIAL_POLICIES = 25000
NEW_POLICIES_PER_YEAR = 5000

PRODUCTS = ["LIFE", "CI", "TERM"]
PLANS = ["LIFE01","LIFE02","CI01","TERM01","TERM02"]
BENEFITS = ["Death","Critical_Illness","Terminal_Illness"]

COUNTRIES = ["UK","US","IN","SG","AU"]


DISTRIBUTION_CHANNELS = [
    "Agency",
    "Broker",
    "Direct",
    "Bancassurance"
]

MONTHS = range(1,13)

policies = []
policy_counter = 1
insured_counter = 1

PRODUCT_PLAN_MAP = {
    "LIFE": ["LIFE01", "LIFE02"],
    "CI": ["CI01"],
    "TERM": ["TERM01", "TERM02"]
}

PRODUCT_BENEFIT_MAP = {
    "LIFE": ["Death"],
    "CI": ["Critical_Illness"],
    "TERM": ["Death", "Terminal_Illness"]
}

RETENTION_LIMIT = 500000
QUOTA_SHARE = 0.70

# -------------------------
# POLICY CREATION FUNCTION
# -------------------------

def calculate_age(dob, valuation_date):
    age = valuation_date.year - dob.year

    if (
        (valuation_date.month, valuation_date.day)
        < (dob.month, dob.day)
    ):
        age -= 1

    return age

def create_policy(issue_year):

    global policy_counter, insured_counter

    insured_id = f"I{random.randint(1,insured_counter):07d}"

    if random.random() < 0.3:
        insured_counter += 1
        insured_id = f"I{insured_counter:07d}"

    product_code = random.choice(PRODUCTS)

    policy = {
        "Policy_num": f"P{policy_counter:08d}",
        "ID_insured": insured_id,
        "Name": fake.name(),
        "DOB": fake.date_of_birth(minimum_age=18, maximum_age=70),
        "gender": random.choice(["M","F"]),
        "resident_country": random.choice(COUNTRIES),
        "Product_code": product_code,
        "Plan_code": random.choice(PRODUCT_PLAN_MAP[product_code]),
        "Benefit_ID": random.choice(PRODUCT_BENEFIT_MAP[product_code]),
        "Issue_date": datetime(issue_year, random.randint(1,12), random.randint(1,28)),
        "sar_base": random.randint(100000, 2000000),
        "Distribution_channel": random.choice(DISTRIBUTION_CHANNELS),
        "active": True
    }

    policy_counter += 1

    return policy


# -------------------------
# INITIAL POLICIES
# -------------------------

for _ in range(INITIAL_POLICIES):
    policies.append(create_policy(random.randint(2014,2017)))

rows = []

# -------------------------
# SIMULATION
# -------------------------

for year in range(START_YEAR, END_YEAR+1):

    # new business
    for _ in range(NEW_POLICIES_PER_YEAR):
        policies.append(create_policy(year))

    for month in MONTHS:

        val_date = datetime(year,month,1)

        for policy in policies:

            if not policy["active"]:
                continue

            if val_date < policy["Issue_date"]:
                continue

            # simulate exits
            exit_prob = random.random()

            if exit_prob < 0.002:
                policy["active"] = False
                continue

            years_active = (
                (val_date - policy["Issue_date"]).days / 365.25
            )

            sar = policy["sar_base"] * (1.02 ** years_active)

            ri_sar = (
                (sar - RETENTION_LIMIT) * QUOTA_SHARE
                if sar > RETENTION_LIMIT
                else sar * QUOTA_SHARE
            )

            age = calculate_age(policy["DOB"], val_date)

            if age >= 80:
                policy["active"] = False
                continue

            rate_per_1000 = 0.5 + (age - 18) * 0.05

            premium_amt = ((ri_sar / 1000) * rate_per_1000)

            new_renew = (
                "New"
                if val_date.year == policy["Issue_date"].year
                and val_date.month == policy["Issue_date"].month
                else "Renew"
            )

            rows.append({
                "Val_date": val_date.strftime("%Y-%m-%d"),
                "Policy_num": policy["Policy_num"],
                "ID_insured": policy["ID_insured"],
                "date_effect_policy": policy["Issue_date"].strftime("%Y-%m-%d"),
                "Product_code": policy["Product_code"],
                "Plan_code": policy["Plan_code"],
                "Benefit_ID": policy["Benefit_ID"],
                "gender": policy["gender"],
                "age": age,
                "premium_amt": round(premium_amt, 2),
                "ri_sar": round(ri_sar, 2),
                "SAR": round(sar, 2),
                "RI_SHARE%": round((ri_sar / sar) * 100, 2),
                "Issue_date": policy["Issue_date"].strftime("%Y-%m-%d"),
                "DOB": policy["DOB"].strftime("%Y-%m-%d"),
                "New/Renew": new_renew,
                "Name": policy["Name"],
                "Distribution_channel": policy["Distribution_channel"],
                "resident_country": policy["resident_country"]
            })

# -------------------------
# DATAFRAME
# -------------------------

df = pd.DataFrame(rows)

print("Total rows:", len(df))

# -------------------------
# SAVE QUARTERLY FILES
# -------------------------

df["year"] = pd.to_datetime(df["Val_date"]).dt.year
df["quarter"] = pd.to_datetime(df["Val_date"]).dt.quarter

for (y,q), data in df.groupby(["year","quarter"]):

    file_name = f"data/incoming/premium/premium_{y}_Q{q}.csv"

    data.drop(columns=["year","quarter"]).to_csv(file_name, index=False)

    print("Saved:", file_name)