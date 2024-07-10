import pandas as pd

# pre-processing
trans = pd.read_csv("/Users/shion/DE test/sample.csv")
trans.columns = trans.columns.str.lower()
trans = trans.sort_values(by=["shop_id", "date"], ascending=True)
trans["lower_range"] = pd.to_datetime(trans["date"])

# calculating gap and status
trans["upper_range"] = trans.groupby("shop_id")["lower_range"].shift(-1)
trans["gap"] = trans["upper_range"] - trans["lower_range"]
trans["status"] = trans["gap"].apply(
    lambda x: "clsd" if x >= pd.Timedelta(days=30) else "open"
)

# Aggregate transactions
agg_trans = (
    trans.groupby(["shop_id", "status"])
    .agg(lower_range=("lower_range", "min"), upper_range=("upper_range", "max"))
    .reset_index()
)
agg_trans = agg_trans.sort_values(by=["shop_id", "lower_range"], ascending=True)

# Add previous and next status columns
agg_trans = agg_trans.sort_values(by=["shop_id", "lower_range"]).reset_index(drop=True)
agg_trans["prev_status"] = agg_trans.groupby("shop_id")["status"].shift(1)
agg_trans["next_status"] = agg_trans.groupby("shop_id")["status"].shift(-1)

# Include separate record with NULL upper_range if applicable
null_upper_range = trans[(trans["upper_range"].isnull())][
    ["shop_id", "status", "lower_range", "upper_range"]
]
agg_df = pd.concat([agg_trans, null_upper_range])

# Add previous and next status columns
agg_df = agg_df.sort_values(by=["shop_id", "lower_range"]).reset_index(drop=True)
agg_df["prev_status"] = agg_df.groupby("shop_id")["status"].shift(1)
agg_df["next_status"] = agg_df.groupby("shop_id")["status"].shift(-1)


# Calculate corrected lower_range and upper_range
def calculate_ranges(row):
    if row["status"] == "open" and row["prev_status"] == "clsd":
        row["lower_range"] = agg_df.loc[
            (agg_df["shop_id"] == row["shop_id"]) & (agg_df["status"] == "clsd"),
            "upper_range",
        ].values[0]

    elif row["status"] == "open" and row["next_status"] == "clsd":
        row["upper_range"] = agg_df.loc[
            (agg_df["shop_id"] == row["shop_id"]) & (agg_df["status"] == "clsd"),
            "lower_range",
        ].values[0]

    elif (
        row["status"] == "open"
        and pd.isnull(row["next_status"])
        and pd.to_datetime(row["lower_range"]) < pd.Timestamp("2022-12-31")
    ):
        row["status"] = "clsd"
        row["lower_range"] += pd.Timedelta(days=1)

    elif (
        row["status"] == "open"
        and row["next_status"] == "open"
        and pd.to_datetime(row["upper_range"]) == pd.Timestamp("2022-12-31")
    ):
        row["upper_range"] = pd.NaT

    elif row["status"] == "open" and row["prev_status"] == "clsd":
        row["lower_range"] = agg_df.loc[
            (agg_df["shop_id"] == row["shop_id"]) & (agg_df["status"] == "clsd"),
            "upper_range",
        ].values[0]

    elif row["status"] == "clsd" and row["prev_status"] == "open":
        row["lower_range"] += pd.Timedelta(days=1)
        row["upper_range"] -= pd.Timedelta(days=1)

    return row


final_df = agg_df.apply(calculate_ranges, axis=1)[
    ["shop_id", "status", "lower_range", "upper_range"]
]

# Additional Filter: Remove records where lower_range is '2012-12-31' and status is 'open'
final_df = final_df[
    ~((final_df["lower_range"] == "2022-12-31") & (final_df["status"] == "open"))
]

final_df.to_csv("pizza_python.csv")
