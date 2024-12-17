import streamlit as st
import polars as pl

from utils import duck

st.set_page_config(layout="wide")

md_preds = duck.md_con.sql(duck.relations["preds_rel"])
picker = duck.Picker(duck.md_con, md_preds)

@st.cache_data(ttl=3600)
def get_preds_for_horizon():
    return picker.get_preds_per_horizon()

df_preds_per_horizon = get_preds_for_horizon()

# First row of filters
row1_cols = st.columns(3)

# Model horizon filter
horizons = df_preds_per_horizon["pred_horizon"].unique().sort()
selected_horizons = row1_cols[0].multiselect(
    "Model Horizons",
    options=horizons,
    default=horizons,
)

# Sector and Industry filters
sectors = df_preds_per_horizon["sector"].unique().sort()
selected_sectors = row1_cols[1].multiselect(
    "Sectors (none = all)",
    options=sectors,
    default=[],
)

industries = df_preds_per_horizon["industry"].unique().sort() if not selected_sectors else df_preds_per_horizon.filter(pl.col("sector").is_in(selected_sectors))["industry"].unique().sort()
selected_industries = row1_cols[2].multiselect(
    "Industries (none = all)",
    options=industries,
    default=[],
)

# Second row of filters
row2_cols = st.columns(2)

# Value range filters
min_pred_value = float(df_preds_per_horizon["predicted_value"].min())
max_pred_value = float(df_preds_per_horizon["predicted_value"].max())
pred_value_range = row2_cols[0].slider(
    "Predicted Value Range",
    min_value=min_pred_value,
    max_value=max_pred_value,
    value=(min_pred_value, max_pred_value),
)

min_std = float(df_preds_per_horizon["predicted_std"].min())
max_std = float(df_preds_per_horizon["predicted_std"].max())
std_range = row2_cols[1].slider(
    "Standard Deviation Range",
    min_value=min_std,
    max_value=max_std,
    value=(min_std, max_std),
)

# Apply filters
df_filtered = df_preds_per_horizon.filter(
    (pl.col("pred_horizon").is_in(selected_horizons)) &
    (pl.col("predicted_value").is_between(pred_value_range[0], pred_value_range[1])) &
    (pl.col("predicted_std").is_between(std_range[0], std_range[1])) &
    (
        (pl.col("sector").is_in(selected_sectors)) if len(selected_sectors) > 0
        else pl.lit(True)
    ) &
    (
        (pl.col("industry").is_in(selected_industries)) if len(selected_industries) > 0
        else pl.lit(True)
    )
)

event = st.dataframe(
    df_filtered,
    use_container_width=True,
    selection_mode="multi-row",
    on_select="rerun"
)

rows: list = event.selection["rows"]

df_selection = (
    df_filtered
    .with_row_index()
    .filter(pl.col("index").is_in(rows))
    .drop("index")
)

if len(rows) > 0:
    st.dataframe(df_selection)

    picked: list[list[str]] = (
        df_selection
        .select(["ticker", "pred_col"])
        .rows()
    )

    cols = st.columns(len(picked))

    for i, x in enumerate(cols):
        x.write(f"Ticker: {picked[i][0]} | Horizon: {picked[i][1][-3:]}")
        x.dataframe(
            picker.get_shaps(picked[i][0], picked[i][1]),
            height=800
        )
