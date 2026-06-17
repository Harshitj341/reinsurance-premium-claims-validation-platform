import pandas as pd
import random
import glob
from datetime import timedelta

# -----------------------------
# LOAD PREMIUM FILES
# -----------------------------

premium_files = glob.glob("data/incoming/premium/*.csv")

premium_list = []

for f in premium_files:
    premium_list.append(pd.read_csv(f))

premium = pd.concat(premium_list, ignore_index=True)

premium["Val_date"] = pd.to_datetime(premium["Val_date"])

premium["date_effect_policy"] = pd.to_datetime(
    premium["date_effect_policy"]
)


premium = premium.sort_values(["Policy_num","Val_date"])

print("Premium rows loaded:", len(premium))


# -----------------------------
# INDEX PREMIUM BY POLICY
# -----------------------------

policy_groups = {
    policy: df.sort_values("Val_date").reset_index(drop=True)
    for policy, df in premium.groupby("Policy_num")
}

policies = list(policy_groups.keys())


# -----------------------------
# HELPER FUNCTIONS
# -----------------------------

def quarter_bounds(year, q):

    if q == 1:
        return pd.Timestamp(f"{year}-01-01"), pd.Timestamp(f"{year}-03-31")
    if q == 2:
        return pd.Timestamp(f"{year}-04-01"), pd.Timestamp(f"{year}-06-30")
    if q == 3:
        return pd.Timestamp(f"{year}-07-01"), pd.Timestamp(f"{year}-09-30")

    return pd.Timestamp(f"{year}-10-01"), pd.Timestamp(f"{year}-12-31")


def get_premium_before_loss(policy_rows, loss_date):

    dates = pd.to_datetime(policy_rows["Val_date"])

    idx = dates.searchsorted(loss_date, side="right") - 1

    if idx < 0:
        return None

    return policy_rows.iloc[idx]


# -----------------------------
# CLAIM GENERATION
# -----------------------------

claims = []
claim_counter = 1

START_YEAR = 2015
END_YEAR = 2025

for year in range(START_YEAR, END_YEAR + 1):

    for q in [1,2,3,4]:

        quarter_start, quarter_end = quarter_bounds(year,q)

        reporting_period = f"{year}Q{q}"

        # portfolio-driven claim volume
        quarter_rows = premium[
            (premium["Val_date"] >= quarter_start) &
            (premium["Val_date"] <= quarter_end)
        ]

        policies_in_force = quarter_rows["Policy_num"].unique()

        claim_volume = int(len(policies_in_force) * random.uniform(0.01,0.03))

        for _ in range(claim_volume):

            policy = random.choice(policies_in_force)

            policy_rows = policy_groups[policy]

            # reported date inside quarter
            policy_start = pd.to_datetime(
                policy_rows["date_effect_policy"]
            ).min()

            effective_start = max(
                quarter_start,
                policy_start
            )

            if effective_start > quarter_end:
                continue

            reported_date = effective_start + timedelta(
                days=random.randint(
                    0,
                    (quarter_end - effective_start).days
                )
            )

            policy_start = pd.to_datetime(
                policy_rows["date_effect_policy"]
            ).min()

            loss_start = max(
                reported_date - timedelta(days=365),
                policy_start
            )

            loss_date = loss_start + timedelta(
                days=random.randint(
                    0,
                    (reported_date - loss_start).days
                )
            )

            premium_row = get_premium_before_loss(policy_rows, loss_date)

            if premium_row is None:
                continue

            claim_amount = round(
                random.uniform(0.3, 1.0) * premium_row["ri_sar"],
                2
            )

            claim = {
                "claim_id": f"C{claim_counter:08d}",
                "reporting_period": reporting_period,

                "Policy_num": premium_row["Policy_num"],
                "ID_insured": premium_row["ID_insured"],

                "Product_code": premium_row["Product_code"],
                "Plan_code": premium_row["Plan_code"],
                "Benefit_ID": premium_row["Benefit_ID"],

               "date_effect_policy":
                pd.to_datetime(
                    premium_row["date_effect_policy"]
                ).strftime("%Y-%m-%d"),

                "loss_date":
                    loss_date.strftime("%d-%m-%Y"),

                "reported_date":
                    reported_date.strftime("%d-%m-%Y"),

                "claim_amount":
                    round(claim_amount, 2),

                "resident_country":
                    premium_row["resident_country"]
            }
            claims.append(claim)

            claim_counter += 1




# -----------------------------
# SAVE CLAIM FILES
# -----------------------------

claims_df = pd.DataFrame(claims)

claims_df["reported_date"] = pd.to_datetime(
    claims_df["reported_date"], format="%d-%m-%Y"
)

claims_df["year"] = claims_df["reported_date"].dt.year
claims_df["quarter"] = claims_df["reported_date"].dt.quarter

for (y,q), data in claims_df.groupby(["year","quarter"]):

    file = f"data/incoming/claims/claims_{y}_Q{q}.csv"

    data.drop(columns=["year","quarter"]).to_csv(file,index=False)

    print("Saved:", file)


print("Total claims generated:", len(claims_df))