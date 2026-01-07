import streamlit as st
import pandas as pd
import os
import io
# -------------------------------
# Load integrated dataset
# -------------------------------
processed_dir = "/mnt/d/WSLProjects/my-etl-pipeline/my_etl_project/data/processed"
output_file = os.path.join(processed_dir, "integrated_dataset.parquet")

@st.cache_data
def load_data():
    return pd.read_parquet(output_file)

df = load_data()

# -------------------------------
# Dashboard Layout
# -------------------------------
st.title("📊 Integrated Data Dashboard")

# Sidebar filters
st.sidebar.header("Filters")

# Date filter (if 'date' column exists)
if "date" in df.columns:
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    min_date, max_date = df["date"].min(), df["date"].max()
    date_range = st.sidebar.date_input(
        "Select Date Range",
        [min_date, max_date],
        min_value=min_date,
        max_value=max_date
    )
    if len(date_range) == 2:
        start, end = date_range
        df = df[(df["date"] >= pd.to_datetime(start)) & (df["date"] <= pd.to_datetime(end))]

# Column selector
selected_col = st.sidebar.selectbox("Choose a column to visualize", df.columns)

# Chart type selector
chart_type = st.sidebar.radio("Chart Type", ["Line", "Bar", "Area"])

# -------------------------------
# KPIs
# -------------------------------
st.subheader("🔑 Key Metrics")
col1, col2, col3 = st.columns(3)
col1.metric("Total Rows", len(df))
col2.metric("Unique Dates", df["date"].nunique() if "date" in df.columns else "N/A")
col3.metric("Missing Values", df.isnull().sum().sum())

# -------------------------------
# Raw Data Preview
# -------------------------------
st.write("### 📄 Raw Data Preview")
st.dataframe(df.head(20))

# -------------------------------
# Visualization
# -------------------------------
st.write(f"### 📈 Visualization of `{selected_col}`")

if chart_type == "Line":
    st.line_chart(df[selected_col])
elif chart_type == "Bar":
    st.bar_chart(df[selected_col])
elif chart_type == "Area":
    st.area_chart(df[selected_col])

# -------------------------------
# Summary Statistics
# -------------------------------
st.write("### 📊 Data Summary")
st.write(df.describe(include="all"))

# -------------------------------
# Refresh Button
# -------------------------------
if st.button("🔄 Refresh Data"):
    df = load_data()
    st.success("Data reloaded successfully!")
# -------------------------------
# Download Button
# -------------------------------
st.write("### 📥 Download Filtered Data")

csv = df.to_csv(index=False).encode("utf-8")
st.download_button(
    label="Download as CSV",
    data=csv,
    file_name="filtered_dataset.csv",
    mime="text/csv"
)

