"""Generate Excel scorecard with per-segment delta % from winsorized data.

Reads existing JS data for title/segment metrics, queries Databricks for
the winsorized off-title delta (treatment - control TVT per viewer),
and produces an Excel with a Delta column per segment.
"""

import json
import re
import pandas as pd
from databricks.sdk import WorkspaceClient
from databricks import sql as dbsql

# --- Parse JS data ---
with open("/Users/lmantena/content-incrementality-scorecard/v8_28d_7d_data.js") as f:
    js_text = f.read()

# Extract the array from "const ALL = [...];"
match = re.search(r"const ALL = (\[.*?\]);", js_text, re.DOTALL)
raw = match.group(1)
# Fix JS nulls -> Python None
raw = raw.replace("null", "None")
ALL = eval(raw)

# Build DataFrame from JS data
cols = [
    "title_id", "title_name", "seg_idx", "seg_tvt_june1", "rate_28d", "rate_7d",
    "n_pairs", "title_tvt_june1", "ci_lo_28d_hrs", "ci_hi_28d_hrs",
    "ci_lo_7d_hrs", "ci_hi_7d_hrs", "incr_hrs_28d", "incr_hrs_7d",
    "ci_lo_rate_28d", "ci_hi_rate_28d", "ci_lo_rate_7d", "ci_hi_rate_7d",
    "seg_tvt_7d", "title_tvt_7d"
]
df = pd.DataFrame(ALL, columns=cols)

SEG_MAP = {0: "Active non-power (Retained)", 1: "New (M1)", 2: "Non-active (Reactivated)", 3: "Power Viewer"}
SEG_LABELS = {0: "Retained", 1: "New (M1)", 2: "Reactivated", 3: "Power Viewer"}

df["user_segment"] = df["seg_idx"].map(SEG_MAP)
df["title_id"] = df["title_id"].astype(str)

# --- Query Databricks for winsorized delta per title × segment ---
# Delta = avg(winsorized simple_delta_off_T) per viewer, in hours
# And delta_pct = (treat_off_title - ctrl_off_title) / ctrl_off_title * 100

DELTA_SQL = """
WITH percentiles AS (
  SELECT
    title_id,
    user_segment,
    CAST(PERCENTILE(simple_delta_off_T, 0.01) AS DOUBLE) AS delta_p1,
    CAST(PERCENTILE(simple_delta_off_T, 0.99) AS DOUBLE) AS delta_p99
  FROM core_dev.dsa.phase2_event_deltas_v8_7d
  GROUP BY title_id, user_segment
),
winsorized AS (
  SELECT
    ed.title_id,
    ed.user_segment,
    GREATEST(LEAST(CAST(ed.treated_post_tvt_off_title_sec AS DOUBLE),
      CAST(PERCENTILE(ed.treated_post_tvt_off_title_sec, 0.99) OVER (PARTITION BY ed.title_id, ed.user_segment) AS DOUBLE)),
      CAST(PERCENTILE(ed.treated_post_tvt_off_title_sec, 0.01) OVER (PARTITION BY ed.title_id, ed.user_segment) AS DOUBLE)) AS treat_off_wz,
    GREATEST(LEAST(CAST(ed.control_post_tvt_off_title_sec AS DOUBLE),
      CAST(PERCENTILE(ed.control_post_tvt_off_title_sec, 0.99) OVER (PARTITION BY ed.title_id, ed.user_segment) AS DOUBLE)),
      CAST(PERCENTILE(ed.control_post_tvt_off_title_sec, 0.01) OVER (PARTITION BY ed.title_id, ed.user_segment) AS DOUBLE)) AS ctrl_off_wz
  FROM core_dev.dsa.phase2_event_deltas_v8_7d ed
)
SELECT
  CAST(title_id AS STRING) AS title_id,
  user_segment,
  CAST(AVG(treat_off_wz) / 3600.0 AS DOUBLE) AS treat_off_title_hrs,
  CAST(AVG(ctrl_off_wz) / 3600.0 AS DOUBLE) AS ctrl_off_title_hrs,
  CAST((AVG(treat_off_wz) - AVG(ctrl_off_wz)) / 3600.0 AS DOUBLE) AS delta_off_title_hrs,
  CASE WHEN AVG(ctrl_off_wz) > 0
    THEN CAST((AVG(treat_off_wz) - AVG(ctrl_off_wz)) / AVG(ctrl_off_wz) * 100.0 AS DOUBLE)
    ELSE NULL
  END AS delta_pct
FROM winsorized
GROUP BY title_id, user_segment
"""

print("Connecting to Databricks...")
w = WorkspaceClient()
host = w.config.host.replace("https://", "").replace("http://", "").rstrip("/")
WAREHOUSE_ID = "eae4e55a98094e13"

conn = dbsql.connect(
    server_hostname=host,
    http_path=f"/sql/1.0/warehouses/{WAREHOUSE_ID}",
    credentials_provider=lambda: w.config.authenticate,
)

print("Running delta query...")
with conn.cursor() as cur:
    cur.execute(DELTA_SQL)
    delta_cols = [desc[0] for desc in cur.description]
    delta_rows = cur.fetchall()
conn.close()

delta_df = pd.DataFrame(delta_rows, columns=delta_cols)
print(f"Got {len(delta_df)} delta rows")

# --- Merge delta into main data ---
df = df.merge(delta_df[["title_id", "user_segment", "treat_off_title_hrs", "ctrl_off_title_hrs", "delta_off_title_hrs", "delta_pct"]],
              on=["title_id", "user_segment"], how="left")

# --- Build the Excel in rank7d format (June 1-7) ---
# Aggregate per title, pivoting segments
min_pairs = 100

# Filter to titles with min pairs per segment
valid = df[df["n_pairs"] >= min_pairs].copy()

# Compute incremental TVT per row
valid["incr_tvt_7d"] = valid["rate_7d"] * valid["seg_tvt_7d"]

# Pivot to one row per title
titles = valid.groupby(["title_id", "title_name"]).agg(
    title_tvt_7d=("title_tvt_7d", "first"),
).reset_index()

# Add segment-level columns
for si, seg_label in SEG_LABELS.items():
    seg_name = SEG_MAP[si]
    seg_data = valid[valid["seg_idx"] == si][["title_id", "seg_tvt_7d", "rate_7d", "n_pairs", "delta_pct"]].copy()
    seg_data = seg_data.rename(columns={
        "seg_tvt_7d": f"{seg_label}_Seg_TVT",
        "rate_7d": f"{seg_label}_Rate",
        "n_pairs": f"{seg_label}_Pairs",
        "delta_pct": f"{seg_label}_Delta%",
    })
    titles = titles.merge(seg_data, on="title_id", how="left")

# Compute total incremental TVT
seg_tvt_cols = [f"{l}_Seg_TVT" for l in SEG_LABELS.values()]
rate_cols = [f"{l}_Rate" for l in SEG_LABELS.values()]

titles["Incr_TVT_7d"] = 0
for seg_label in SEG_LABELS.values():
    tvt_col = f"{seg_label}_Seg_TVT"
    rate_col = f"{seg_label}_Rate"
    incr = titles[tvt_col].fillna(0) * titles[rate_col].fillna(0)
    titles["Incr_TVT_7d"] += incr
    titles[f"{seg_label}_Incr_TVT"] = incr

titles["Ratio_7d"] = titles["Incr_TVT_7d"] / titles["title_tvt_7d"].replace(0, float("nan"))

# Rank
titles["Incr_TVT_Rank"] = titles["Incr_TVT_7d"].rank(ascending=False, method="min").astype(int)
titles["Title_TVT_Rank"] = titles["title_tvt_7d"].rank(ascending=False, method="min").astype(int)

# Sort by Incr TVT descending
titles = titles.sort_values("Incr_TVT_7d", ascending=False).reset_index(drop=True)

# Arrange final columns
final_cols = ["Incr_TVT_Rank", "Title_TVT_Rank", "title_id", "title_name", "title_tvt_7d", "Incr_TVT_7d", "Ratio_7d"]
for seg_label in ["Power Viewer", "Retained", "Reactivated", "New (M1)"]:
    final_cols += [f"{seg_label}_Seg_TVT", f"{seg_label}_Incr_TVT", f"{seg_label}_Rate", f"{seg_label}_Delta%", f"{seg_label}_Pairs"]

output = titles[final_cols].copy()
output = output.rename(columns={
    "title_id": "Program ID",
    "title_name": "Program Name",
    "title_tvt_7d": "June 1-7 Movie TVT (hrs)",
    "Incr_TVT_7d": "Incr TVT (7d hrs)",
    "Ratio_7d": "Ratio (7d)",
})

# Write to Excel
output_path = "/Users/lmantena/content-incrementality-scorecard/v8_June1_7_scorecard_with_delta.xlsx"
output.to_excel(output_path, index=False, sheet_name="Incrementality Rank June 1-7")
print(f"Written to: {output_path}")
print(f"Titles: {len(output)}, Columns: {list(output.columns)}")
